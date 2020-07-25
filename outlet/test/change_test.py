import unittest


def fun(x):
    return x + 1


class ChangeTest(unittest.TestCase):
    def test(self):
        self.assertEqual(fun(3), 4)
