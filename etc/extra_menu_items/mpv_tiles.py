# Default version of this file: outlet/template/_menu_item.py
import asyncio
import os
import sys
from collections import deque
from functools import partial
from pathlib import Path
from typing import Deque, List, Optional
from model.node.node import Node
import logging
logger = logging.getLogger(__name__)

action_id = 101

APP_NAMES = ['mpv.app', 'mpv2.app', 'mpv3.app', 'mpv4.app']
EXE_PARENT_DIR = '/Applications/$APP_NAME/Contents/MacOS/mpv'

# MPV options: see https://mpv.io/manual/master/
NO_BORDER = '-no-border'
QUIT_WHEN_DONE = '--keep-open=no'
START_FROM_BEGINNING = '--no-resume-playback'
AUTOHIDE_CURSOR_AFTER_1_SEC = '--cursor-autohide=1000'
MUTE_ON = '--mute=yes'
NO_FOCUS_ON_OPEN = f'--no-focus-on-open'

FULLSCREEN = '--fs'
QUARTER_SCREEN_SIZE = f'--autofit=50%x50%'

TOP_LEFT = '--geometry=0%:0%'
TOP_RIGHT = '--geometry=100%:0%'
BOTTOM_LEFT = '--geometry=0%:100%'
BOTTOM_RIGHT = '--geometry=100%:100%'

DEFAULT_CMD_LINE = [NO_BORDER, START_FROM_BEGINNING, QUIT_WHEN_DONE, AUTOHIDE_CURSOR_AFTER_1_SEC, MUTE_ON]

SCREENS = [1, 2]
QUARTER_SCREEN_CMD_LINE_LIST = [[TOP_LEFT], [TOP_RIGHT], [BOTTOM_LEFT], [BOTTOM_RIGHT]]

SUFFIX_SET = {'.mov', '.mp4', '.m2ts', '.mkv', '.avi'}


class ExecState:
    def __init__(self, cmd_line_list, file_deque):
        self.cmd_line_list = cmd_line_list
        self.file_deque: Deque[str] = file_deque
        self.was_cancelled: bool = False


def get_label(node_list: List[Node]) -> str:
    return f'Play Fullscreen on Screen{"" if len(SCREENS) == 1 else "s"} {", ".join([f"#{i + 1}" for i in SCREENS])}'


def is_enabled(node_list: List[Node]) -> bool:
    return True


def run(node_list: List[Node]):
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    file_deque = _find_all_video_files(node_list)

    cmd_line_list = _get_cmd_list_for_two_screens()

    state = ExecState(cmd_line_list, file_deque)
    asyncio.run(_main_loop(state))

    logger.info(f'Done running!')


def _find_all_video_files(node_list: List[Node]) -> Deque[str]:
    file_set = set()

    for node in node_list:
        if node.is_file():
            file_set.add(node.get_single_path())
        elif node.is_dir():
            for root, dirs, files in os.walk(node.get_single_path(), topdown=True):
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    file_set.add(file_path)

    file_deque: Deque[str] = deque()
    for file in file_set:
        suffix = Path(file).suffix
        if suffix in SUFFIX_SET:
            file_deque.append(file)
        else:
            logger.debug(f'Ignoring file: {file}')
    return file_deque


async def _main_loop(state: ExecState):
    logger.info(f'Starting main loop for {len(state.file_deque)} files!')
    task_list = []
    for cmd_line in state.cmd_line_list:
        task = _run_single_instance(cmd_line, state, None)
        if task:
            task_list.append(task)

    # wait for all tasks to finish before returning
    await asyncio.gather(*asyncio.all_tasks() - {asyncio.current_task()})


def _run_single_instance(cmd_line, state: ExecState, task: Optional[asyncio.Task]):
    if task:
        logger.debug(f'Task returned')
        # Log any exceptions, if this was launched after a task ran
        try:
            task.result()
        except asyncio.CancelledError:
            pass  # Task cancellation should not be logged as an error.
        except RuntimeError:
            logging.exception(f'Exception raised by task {task}')

    if state.was_cancelled:
        return None

    if state.file_deque:
        file_path = state.file_deque.popleft()
        logger.debug(f'Processing (remaining: {len(state.file_deque)}) file: {file_path}')
        this_cmd_line = cmd_line + [file_path]
        task = asyncio.create_task(_exec_process_async(this_cmd_line))
        callback = partial(_run_single_instance, cmd_line, state)
        # This will call back immediately if the task already completed:
        task.add_done_callback(callback)
        return task


async def _exec_process_async(cmd_line):
    logger.debug(f'Executing: {cmd_line}')
    process = await asyncio.create_subprocess_exec(
        *cmd_line, stdout=None, stderr=None
    )
    rc = await process.wait()
    return rc


def _get_cmd_list_for_two_screens() -> List[List[str]]:
    cmd_line_list = []

    for index, screen in enumerate(SCREENS):
        exe = EXE_PARENT_DIR.replace('$APP_NAME', APP_NAMES[index])
        cmd_line = [exe, f'-screen={screen}', FULLSCREEN] + DEFAULT_CMD_LINE

        cmd_line_list.append(cmd_line)
    return cmd_line_list


def _get_cmd_list_for_one_screen_4_tiles() -> List[List[str]]:
    cmd_line_list = []

    for index, custom_cmd_line in enumerate(QUARTER_SCREEN_CMD_LINE_LIST):
        exe = EXE_PARENT_DIR.replace('$APP_NAME', APP_NAMES[index])
        cmd_line = [exe, f'-screen={SCREENS[0]}', QUARTER_SCREEN_SIZE, NO_FOCUS_ON_OPEN] + DEFAULT_CMD_LINE + custom_cmd_line

        cmd_line_list.append(cmd_line)
        logger.debug(f'CMD LINE #{index}: {cmd_line}')
    return cmd_line_list
