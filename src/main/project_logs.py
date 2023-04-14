"""
Package for logs generation
"""
import functools
import os
import re
import sys
import logging as log
import datetime as dt


class DefaultLogger:
    """
    Class for Default logging.
    Generates log file in log folder and logs info to console and file
    """
    _logger: log.Logger

    @property
    def logger(self):
        """
        :returns logger
        """
        return self._logger

    @logger.setter
    def logger(self, val):
        """
        logger setter by name (val)
        """
        self._logger = log.getLogger(val)

    def set_default_logger_config(self):
        """
        Setting default config
        """
        self._logger.setLevel(log.INFO)

        # create console handler and set level to debug
        l_stream_handler = log.StreamHandler(sys.stdout)
        l_file_handler = log.FileHandler(self.log_file_name)

        l_file_handler.setLevel(log.INFO)
        l_stream_handler.setLevel(log.INFO)

        # create formatter
        l_formatter = log.Formatter('[ %(asctime)s ] > %(levelname)s: %(message)s')

        # add formatter to l_stream_handler
        l_file_handler.setFormatter(l_formatter)
        l_stream_handler.setFormatter(l_formatter)

        # add l_stream_handler to logger
        self._logger.addHandler(l_stream_handler)
        self._logger.addHandler(l_file_handler)

    def generate_log_file_name(self):
        """
        Generation of unique file name per process
        :return:
        """
        l_pid = os.getpid()
        l_date = str(dt.datetime.today().strftime("%Y%m%d_%H-%M-%S"))

        l_file_name = os.path.basename(self.file)
        l_log_file = f"{l_file_name}_DT_{l_date}_PID_{l_pid}.log"

        if self.log_folder:
            l_log_file = f"{self.log_folder}/{l_log_file}"

        return l_log_file

    def info(self, msg, *args, **kwargs):
        """
        Wrapper for Info
        """
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """
        Wrapper for warning
        """
        self.logger.warning(self, msg, *args, **kwargs)

    def exception(self, msg, *args, exc_info=True, **kwargs):
        """
        Wrapper for exception
        """
        self.logger.exception(msg, *args, exc_info=exc_info, **kwargs)

    def step(self, in_msg, in_major_step: bool = True):
        """
        Prints message surrounded by two lines with 100 ****
        """
        l_100stars = "*" * 100

        if not in_major_step:
            l_dict_msg = [in_msg]
        else:
            l_dict_msg = [l_100stars, in_msg, l_100stars]

        for l_one_msg in l_dict_msg:
            self.logger.info(l_one_msg)

    def start_function(self, in_function_name,
                       in_major_step: bool
                       , *args, **kwargs):
        """"
        Function to show calling function details
        """
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)

        if in_major_step:
            l_start_msg = "Started "
        else:
            l_start_msg = "Executing "

        l_start_msg += in_function_name

        if signature:
            l_start_msg += f" called with args {signature}"

        self.step(l_start_msg, in_major_step)

    def end_function(self, in_function_name,
                     in_major_step: bool,
                     in_exit_code,
                     l_in_dt_start: dt.datetime = None):
        """"
        Function to show function elapsed time and indicate it finish
        """

        l_end_msg = f'Finished {in_function_name} '

        if l_in_dt_start:
            l_diff = dt.datetime.today() - l_in_dt_start
            l_diff_str = re.search("[^.]+", str(l_diff))[0]

            l_end_msg += f"( Elapsed {l_diff_str} ) "

        l_end_msg += f'[[ Exit code : {in_exit_code} ]]'

        self.step(l_end_msg, in_major_step)

    def __init__(self,
                 in_loger_name: str,
                 in_file: str,
                 in_log_folder: str):
        self.file = in_file
        self.log_folder = in_log_folder
        self.logger = in_loger_name
        self.log_file_name = self.generate_log_file_name()
        self.set_default_logger_config()


def add_logging(_func=None, *,
                in_default_logger: DefaultLogger,
                in_major_step: bool = False):
    """
    Decorator to add logging to any existing function
    :param _func:
    :param in_default_logger:
    :param in_major_step: if True will cover message with *** below and above
    :return:
    """

    def decorator_log(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            l_exit_code = 0
            l_dt_start = dt.datetime.today()
            logger = in_default_logger

            l_func_name = func.__name__
            logger.start_function(l_func_name, in_major_step, *args, **kwargs)

            try:
                result = func(*args, **kwargs)
                return result
            except Exception as ex_all:
                logger.exception('Exception raised in %s exception: %s', l_func_name, str(ex_all))
                l_exit_code = 1
                raise ex_all
            finally:
                if in_major_step:
                    logger.end_function(l_func_name, in_major_step, l_exit_code, l_dt_start)

        return wrapper

    if _func is None:
        return decorator_log

    return decorator_log(_func)

# def log(_func=None, *, my_logger: Union[MyLogger, logging.Logger] = None):
#     def decorator_log(func):
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs):
#             logger = get_default_logger()
#             try:
#                 if my_logger is None:
#                     first_args = next(iter(args), None)  # capture first arg to check for `self`
#                     logger_params = [  # does kwargs have any logger
#                                         x
#                                         for x in kwargs.values()
#                                         if isinstance(x, logging.Logger) or isinstance(x, MyLogger)
#                                     ] + [  # # does args have any logger
#                                         x
#                                         for x in args
#                                         if isinstance(x, logging.Logger) or isinstance(x, MyLogger)
#                                     ]
#                     if hasattr(first_args, "__dict__"):  # is first argument `self`
#                         logger_params = logger_params + [
#                             x
#                             for x in first_args.__dict__.values()  # does class (dict) members have any logger
#                             if isinstance(x, logging.Logger)
#                                or isinstance(x, MyLogger)
#                         ]
#                     h_logger = next(iter(logger_params), MyLogger())  # get the next/first/default logger
#                 else:
#                     h_logger = my_logger  # logger is passed explicitly to the decorator
#
#                 if isinstance(h_logger, MyLogger):
#                     logger = h_logger.get_logger(func.__name__)
#                 else:
#                     logger = h_logger
#
#                 args_repr = [repr(a) for a in args]
#                 kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
#                 signature = ", ".join(args_repr + kwargs_repr)
#                 logger.debug(f"function {func.__name__} called with args {signature}")
#             except Exception:
#                 pass
#
#             try:
#                 result = func(*args, **kwargs)
#                 return result
#             except Exception as e:
#                 logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
#                 raise e
#         return wrapper
#
#     if _func is None:
#         return decorator_log
#     else:
#         return decorator_log(_func)
