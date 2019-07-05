import threading
import time
import queue


class MyThread(threading.Thread):

    def __init__(self, threadID, q, name):
        # 因为要覆盖父类的init初始化，所以在最前边调用
        super(MyThread, self).__init__()
        self.q = q
        self.name = name
        self.threadID = threadID

    def run(self):

        print("线程开启{}： ".format(self.name))
        for i in range(100):
            result = process_data(self.name, self.q)
            if result:
                time.sleep(1)
                self.q.task_done()
            else:
                break

        print("线程结束{}： -------".format(self.name))



def process_data(name, q):

    if not q.empty():
        data = q.get()
        print('线程 {} 取出数据---{}'.format(name, data))
        return True
    else:
        print('线程 {} 没有取出数据！！！！'.format(name))
        return False


if __name__ == '__main__':

    qque = queue.Queue(10)
    prefix = 'ceshi-'

    # 向队列中添加数据
    for i in range(10):
        print(i)
        qque.put(i)


    print('队列大小为：',qque.qsize())

    # 定义线程
    thread_list = []
    for i in range(1):
        t = MyThread(i, qque, prefix + str(i))
        thread_list.append(t)

    # 开启线程
    for t in thread_list:
        t.start()


    qque.join()

    print('主线程结束执行')





