import sys
from loguru import logger


def configure_logger():
    # remove default logger
    logger.remove()
    logger.level('DEBUG', color='<white>', icon='[*]')
    logger.level('SUCCESS', color='<green>', icon='[+]')
    logger.level('WARNING', color='<yellow>', icon='[!]')
    logger.level('ERROR', color='<red>', icon='[-]')

    logger.add(sys.stdout, level='DEBUG',
               format='<level><b>{level.icon}</b></level> {message}')
