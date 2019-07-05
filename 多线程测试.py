"""注意：setDaemon()，join(),以及只有start()的区别"""

import threading
import time
import queue

class MyThread(threading.Thread):

    def __init__(self, threadID, name):
        # threading.Thread.__init__(self)
        super(MyThread, self).__init__()
        # self.q = q
        self.name = name
        self.threadID = threadID

    def run(self):

        print("线程开启{}： {}".format(self.name, self.threadID))
        for i in range(3):
            print("线程{}： 正在执行中.....{}".format(self.name, i+1))
            time.sleep(2)

        print("线程结束{}： {}".format(self.name, self.threadID))


# if __name__ == '__main__':
#
#     prefix = 'ceshi-'
#
#     # q = queue.Queue()
#     thread_list = []
#     for i in range(3):
#         t = MyThread(i, prefix + str(i))
#         thread_list.append(t)
#
#     for t in thread_list:
#         t.setDaemon(True)
#         t.start()
#         t.join(timeout=1)
#
#     # for i in range(5):
#     #     print(i)
#     #     time.sleep(1)
#
#     print('主线程结束执行')

from rest_framework import exceptions


def pt():


    for i in range(3):
        try:

            if i == 2:

                error = "eee"
                raise ValueError(error)
            print("1213123")
        except Exception as e:
            print(e)
            print(e.args)
            print(type(e))
            pass

        print("12121212121212")


import json
if __name__ == '__main__':

    pt()

