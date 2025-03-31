import configparser

config = configparser.ConfigParser()
config.read("config.ini")

REDIS_HOST = config.get("REDIS", "host")
REDIS_PORT = config.getint("REDIS", "port")
ACTIVITY_THRESHOLD = config.getint("BOT", "activity_threshold")
HDFS_PATH = config.get("HDFS", "path")