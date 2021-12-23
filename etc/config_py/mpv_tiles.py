# Default version of this file: outlet/template/_menu_item.py
import asyncio
import sys
from functools import partial
from typing import List, Optional
from model.node.node import Node
import logging
logger = logging.getLogger(__name__)

action_id = 101

APP_NAMES = ['mpv.app', 'mpv2.app', 'mpv3.app', 'mpv4.app']
EXE_PARENT_DIR = '/Applications/$APP_NAME/Contents/MacOS/mpv'

# MPV options: see https://mpv.io/manual/master/
FULLSCREEN = '--fs'
NO_BORDER = '-no-border'
QUIT_WHEN_DONE = '--keep-open=no'
START_FROM_BEGINNING = '--no-resume-playback'
QUARTER_SCREEN_SIZE = f'--autofit=50%x50%'
AUTOHIDE_CURSOR_AFTER_1_SEC = '--cursor-autohide=1000'
MUTE_ON = '--mute=yes'

DEFAULT_CMD_LINE = [NO_BORDER, START_FROM_BEGINNING, QUIT_WHEN_DONE, AUTOHIDE_CURSOR_AFTER_1_SEC, MUTE_ON]

SCREENS = [1, 2]


class ExecState:
    def __init__(self, cmd_line_list, node_list):
        self.cmd_line_list = cmd_line_list
        self.node_list: List[Node] = node_list
        self.was_cancelled: bool = False


def get_label(node_list: List[Node]) -> str:
    return f'Tile with MPV for {len(SCREENS)} Screens'


def is_enabled(node_list: List[Node]) -> bool:
    return True


def run(node_list: List[Node]):
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    # if len(SCREENS) == 2:
    cmd_line_list = _get_cmd_list_for_two_screens(node_list)

    state = ExecState(cmd_line_list, node_list)
    asyncio.run(_main_loop(state))

    logger.info(f'Done running!')


async def _main_loop(state: ExecState):
    logger.info(f'Starting main loop!')
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

    if state.node_list:
        node = state.node_list.pop()
        logger.debug(f'Processing (remaining: {len(state.node_list)}) node: {node.node_identifier}')
        this_cmd_line = cmd_line + [node.node_identifier.get_single_path()]
        task = asyncio.create_task(_play_one(this_cmd_line))
        callback = partial(_run_single_instance, cmd_line, state)
        # This will call back immediately if the task already completed:
        task.add_done_callback(callback)
        return task


async def _play_one(cmd_line):
    logger.debug(f'Executing: {cmd_line}')
    process = await asyncio.create_subprocess_exec(
        *cmd_line, stdout=None, stderr=None
    )
    rc = await process.wait()
    return rc


def _get_cmd_list_for_two_screens(node_list: List[Node]) -> List[List[str]]:
    cmd_line_list = []

    for index, screen in enumerate(SCREENS):
        exe = EXE_PARENT_DIR.replace('$APP_NAME', APP_NAMES[index])
        cmd_line = [exe, f'-screen={screen}', FULLSCREEN] + DEFAULT_CMD_LINE

        cmd_line_list.append(cmd_line)
        # output = subprocess.check_output(cmd_line).decode().split('\n')[0]

        # execute(*runners(cmds))
    return cmd_line_list


async def _read_stream(stream, cb):
    while True:
        line = await stream.readline()
        if line:
            cb(line)
        else:
            break
