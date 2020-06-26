import os
import time
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class Watcher:
    DIRECTORY_TO_WATCH = "/tmp/"

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Error")

        self.observer.join()


know_files = set()


class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):

        if event.src_path.endswith(".hpl") and event.src_path.startswith("/tmp/exa"):
            if event.src_path in know_files:
                return
            print(event.src_path)
            know_files.add(event.src_path)
            os.link(event.src_path, '/tmp/profile' + str(len(know_files)) + ".hpl")


if __name__ == '__main__':
    w = Watcher()
    w.run()
