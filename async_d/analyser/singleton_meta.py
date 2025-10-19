import threading

class SingletonMeta(type):
    _instances = {}

    def __new__(cls, name, bases, namespace):
        new_class = super().__new__(cls, name, bases, namespace)
        new_class._lock = threading.RLock()  # 每个类独立的锁
        return new_class

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:  # 第一次无锁检查
            with cls._lock:
                if cls not in cls._instances:  # 第二次加锁检查
                    cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]