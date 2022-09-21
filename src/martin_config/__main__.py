import os
import logging


if __name__ == '__main__':
    logging.basicConfig()
    sthandler = logging.StreamHandler()
    sthandler.setFormatter(logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s', "%Y-%m-%d %H:%M:%S"))
    logger = logging.getLogger()
    # remove the default stream handler and add the new on too it.
    logger.handlers.clear()
    logger.addHandler(sthandler)

    logger.setLevel('INFO')
    logger.name = os.path.split(__file__)[-1]
    logger.info(f'{"*" * 30} START {"*" * 30} ')