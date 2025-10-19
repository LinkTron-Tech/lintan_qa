
import os, sys, logging, queue
from logging.handlers import QueueHandler, QueueListener, TimedRotatingFileHandler, RotatingFileHandler

def setup_logging():
    # 1. 准备目录和文件路径

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    LOG_DIR  = os.getenv("LOG_DIR", os.path.join(BASE_DIR, 'logs'))
    os.makedirs(LOG_DIR, exist_ok=True)
    INFO_FILE    = os.path.join(LOG_DIR, "info.log")
    WARNING_FILE = os.path.join(LOG_DIR, "warning.log")
    ERROR_FILE   = os.path.join(LOG_DIR, "error.log")
    DEBUG_FILE   = os.path.join(LOG_DIR, "debug.log")

    # 2. 配置真正的写入 handler（文件 + 控制台）
    fmt = "%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s:%(lineno)d]\n%(message)s\n"
    datefmt = "%Y-%m-%d-%H:%M:%S"
    console_h   = logging.StreamHandler(sys.stdout)
    console_h.setLevel(logging.INFO)
    console_h.setFormatter(logging.Formatter(fmt, datefmt))

    info_h    = TimedRotatingFileHandler(INFO_FILE, when="midnight", backupCount=7, encoding="utf-8")
    info_h.setLevel(logging.INFO)
    warning_h = RotatingFileHandler(WARNING_FILE, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
    warning_h.setLevel(logging.WARNING)
    error_h   = RotatingFileHandler(ERROR_FILE,   maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
    error_h.setLevel(logging.ERROR)
    debug_h   = RotatingFileHandler(DEBUG_FILE,   maxBytes=20*1024*1024, backupCount=3, encoding="utf-8")
    debug_h.setLevel(logging.DEBUG)

    for h in (info_h, warning_h, error_h, debug_h):
        h.setFormatter(logging.Formatter(fmt, datefmt))

    # 3. 把所有“真实写入” handler 放进一个队列监听器
    log_queue = queue.Queue(-1)  # 无限长度
    queue_handler = QueueHandler(log_queue)
    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)
    root.addHandler(queue_handler)

    if os.environ.get("ENV") == "prod":
        # 生产环境不需要打印debug日志
        listener = QueueListener(
        log_queue,
        console_h, info_h, warning_h, error_h,
        respect_handler_level=True
    )
    else:
        LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
        listener = QueueListener(
            log_queue,
            console_h, info_h, warning_h, error_h, debug_h,
            respect_handler_level=True
        )
    listener.daemon = True  # 随主线程退出
    listener.start()

    # 4. 可选：针对某些第三方库降级日志级别
    for name in ("google_genai.models", "openai", "httpx"):
        logging.getLogger(name).setLevel("WARNING")