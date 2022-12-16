import threading

from douban_novel import start_crawle_novel
from tonghuashun_blocktrade import start_crawle_blockTrade

if __name__ == '__main__':
    ths_blocktrade_thread = threading.Thread(target=start_crawle_blockTrade,
                                             name='thread_crawler_tonghuashun_blocktrade')
    db_novel_thread = threading.Thread(target=start_crawle_novel, name='thread_crawler_douban_novel')
    ths_blocktrade_thread.start()
    db_novel_thread.start()

    # 等待子进程执行完毕
    ths_blocktrade_thread.join()
    db_novel_thread.join()
