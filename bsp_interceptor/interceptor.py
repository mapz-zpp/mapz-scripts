import dataclasses
import json
import pickle
import shutil
import socket
import subprocess
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, StrEnum
from pathlib import Path
from subprocess import Popen
from threading import Event, Thread
from time import sleep
from typing import TextIO

INTERCEPTOR_HOST = "127.0.0.1"
INTERCEPTOR_PORT = 45555

# ===== COMMON STRUCTURE =====

class Sender(StrEnum):
    CLIENT = "Client [IntelliJ]"
    SERVER = "Server"

    def color(self) -> str:
        if self == Sender.SERVER:
            return "\033[31m"
        else:
            return "\033[32m"

@dataclass
class InterceptorMessage:
    time: datetime
    message: dict
    sender: Sender


# ===== INTERCEPTOR SERVER =====

class InterceptorAggregator:
    messages: list[InterceptorMessage]

    def __init__(self):
        self.messages = []

    @staticmethod
    def recvn(stream: socket.socket, length: int) -> bytes | None:
        if length == 0:
            raise RuntimeError("Cannot receive 0!")

        buffer = bytes()
        while length > 0:
            received_part = stream.recv(length)
            if not received_part:
                return None
            buffer += received_part
            length -= len(received_part)

        return buffer if buffer else None

    def run_printer(self):
        first_index_to_print = 0
        while True:
            sleep(1)
            my_messages = self.messages.copy()
            messages = my_messages[first_index_to_print:]
            first_index_to_print = len(my_messages)

            for message in messages:
                print(f"{message.sender.color()}[{message.time} - {message.sender}] Intercepted BSP message\n{json.dumps(message.message, indent=2)}")

    def run_receiver(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp_socket:
            tcp_socket.bind((INTERCEPTOR_HOST, INTERCEPTOR_PORT))
            tcp_socket.listen()

            while True:
                conn, _ = tcp_socket.accept()

                while True:
                    conn_length_bytes = self.recvn(conn, 64)
                    if not conn_length_bytes:
                        break

                    conn_length = int.from_bytes(conn_length_bytes)
                    pickled_object = self.recvn(conn, conn_length)
                    if not pickled_object:
                        break

                    messages: list[InterceptorMessage] = pickle.loads(pickled_object)
                    messages.sort(key=lambda x: x.time)
                    self.messages.extend(messages)



# ===== SERVER MIMIC =====

class InterceptorStorage:
    client_messages: list[InterceptorMessage]
    server_messages: list[InterceptorMessage]

    def __init__(self):
        self.server_messages = []
        self.client_messages = []

    def push_client_message(self, message: InterceptorMessage):
        self.client_messages.append(message)

    def push_server_message(self, message: InterceptorMessage):
        self.server_messages.append(message)


class JSONRpcReader:
    stream: TextIO

    def __init__(self, stream: TextIO):
        self.stream = stream

    def readline(self) -> str:
        return self.stream.readline()

    def read_message(self) -> tuple[str, dict]:
        headers = ""
        content_length = None

        line = "first_line"
        while line and line.strip():
            line = self.readline()
            headers += line
            if "Content-Length" in line:
                content_length = int(line.split(":")[-1].strip())

        message = ""
        while content_length > 0:
            partial_message = self.stream.read(content_length)
            message += partial_message
            content_length -= len(partial_message)

        raw_message = f"{headers}{message}"
        return raw_message, json.loads(message)


def listen_to_server_task(storage: InterceptorStorage, stream: TextIO, end_event: Event):
    process_reader = JSONRpcReader(stream)
    while True:
        try:
            message, parsed = process_reader.read_message()
            server_message = InterceptorMessage(
                message=parsed,
                time=datetime.now(),
                sender=Sender.SERVER
            )
            storage.push_server_message(server_message)
            sys.stdout.write(message)
            sys.stdout.flush()
        except Exception as e:
            print(f"EXCEPTION FROM SBT {traceback.format_exception(e)}")
            end_event.set()

def listen_to_client_task(storage: InterceptorStorage, stream: TextIO, end_event: Event):
    stdin_reader = JSONRpcReader(sys.stdin)
    while True:
        try:
            message, parsed = stdin_reader.read_message()
            client_message = InterceptorMessage(
                message=parsed,
                time=datetime.now(),
                sender=Sender.CLIENT
            )
            storage.push_client_message(client_message)
            stream.write(message)
            stream.flush()
            if parsed.get("method") == "build/exit":
                end_event.set()
        except Exception as e:
            print(f"EXCEPTION FROM SBT {traceback.format_exception(e)}", file=sys.stderr)
            end_event.set()

def sender_task(storage: InterceptorStorage, send_event: Event):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp_socket:
        try:
            tcp_socket.connect((INTERCEPTOR_HOST, INTERCEPTOR_PORT))
        except Exception as e:
            print(f"Failed to connect remote aggregator, exc_info={traceback.format_exception(e)}", file=sys.stderr)
            return

        first_client_index = 0
        first_server_index = 0

        while True:
            sleep(1)
            client_messages = storage.client_messages.copy()
            server_messages = storage.server_messages.copy()

            to_send = client_messages[first_client_index:] + server_messages[first_server_index:]
            pickled_data = pickle.dumps(to_send)
            pickle_length = len(pickled_data).to_bytes(64)

            first_client_index = len(client_messages)
            first_server_index = len(server_messages)

            try:
                tcp_socket.sendall(pickle_length + pickled_data)
            except Exception as e:
                print(f"EXCEPTION while sending pickled_data, exc_info={traceback.format_exception(e)}", file=sys.stderr)

            send_event.set()



def mimic():
    server_argv = ["/opt/homebrew/opt/sdkman-cli/libexec/candidates/java/17.0.9-tem/bin/java","-Xms100m","-Xmx100m","-classpath","/opt/homebrew/Cellar/sbt/1.9.9/libexec/bin/sbt-launch.jar","-Dsbt.script=/opt/homebrew/Cellar/sbt/1.9.9/libexec/bin/sbt","xsbt.boot.Boot","-bsp"]
    process = Popen(server_argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=sys.stderr, text=True)

    storage = InterceptorStorage()
    end_event = Event()
    send_event = Event()

    t1 = Thread(target=listen_to_server_task, args=(storage, process.stdout, end_event))
    t1.daemon = True
    t1.start()

    t2 = Thread(target=listen_to_client_task, args=(storage, process.stdin, end_event))
    t2.daemon = True
    t2.start()

    t3 = Thread(target=sender_task, args=(storage, send_event))
    t3.daemon = True
    t3.start()

    end_event.wait()
    sleep(2)

    send_event.clear()
    send_event.wait()

    sys.exit(0)

# ===== REPLACER ======


    {"name": "sbt",
     "version": "1.9.8",
     "languages": ["scala"],
     "argv": ["python", "-u", "/Users/pfuchs/Bachelor/ZPP/bsphelloworld/../bsp_inceptor/inceptor.py", "mimic", "/Users/pfuchs/Bachelor/ZPP/bsphelloworld/../bsp_inceptor/inceptor.py"],
     "bspVersion": "2.1.0-M1"}


POSSIBLE_BSP_FILE_NAMES = ["sbt.json"]

@dataclass
class BSPConnectionDetails:
    name: str
    version: str
    languages: list[str]
    argv: list[str]
    bsp_version: str

    def dump_to_connection_file(self, connection_file_path: Path):
        self_dict = dataclasses.asdict(self)
        if "bsp_version" in self_dict:
            bsp_version = self_dict.pop("bsp_version")
            self_dict["bspVersion"] = bsp_version

        self_json = json.dumps(self_dict)
        connection_file_path.write_text(self_json)

    @staticmethod
    def parse_connection_file(connection_file_path: Path) -> "BSPConnectionDetails":
        file_content = connection_file_path.read_text()
        file_json: dict = json.loads(file_content)
        if "bspVersion" in file_json:
            bsp_version = file_json.pop("bspVersion")
            file_json["bsp_version"] = bsp_version

        return BSPConnectionDetails(**file_json)

    def to_script_connection_details(self, script_path: Path) -> "BSPConnectionDetails":
        return BSPConnectionDetails(
            name=self.name,
            version=self.version,
            languages=self.languages.copy(),
            argv=["python", "-u", str(script_path), "mimic"],
            bsp_version=self.bsp_version
        )



def replace(path: Path):
    connection_details: BSPConnectionDetails | None = None
    possible_path: Path | None = None

    for possible_name in POSSIBLE_BSP_FILE_NAMES:
        possible_path = path / ".bsp" / possible_name
        if possible_path.is_file():
            connection_details = BSPConnectionDetails.parse_connection_file(possible_path)

    if not connection_details:
        raise RuntimeError("Did not find connection details file")

    interceptor_path = path / ".bsp_interceptor"
    interceptor_path.mkdir(parents=True, exist_ok=True)

    copied_script_path = interceptor_path / "interceptor_clone.py"
    shutil.copy(__file__, copied_script_path)

    script_connection_details = connection_details.to_script_connection_details(copied_script_path.absolute())
    script_connection_details.dump_to_connection_file(possible_path)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        aggr = InterceptorAggregator()
        t = Thread(target=aggr.run_printer)
        t.daemon = True
        t.start()

        aggr.run_receiver()
    elif sys.argv[1] == "mimic":
        mimic()
    else:
        path = Path(sys.argv[2])
        replace(path)
