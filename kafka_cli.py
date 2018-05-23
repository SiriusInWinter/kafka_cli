# coding=utf-8
"""
kafka cli
"""
from __future__ import print_function
import re
import sys
import json
import time
import getopt
import signal
import readline

# kafka
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition


class Colorizing(object):
    """
    colorizing
    """
    colors = {
        'none': "",
        'default': "\033[0m",
        'bold': "\033[1m",
        'underline': "\033[4m",
        'blink': "\033[5m",
        'reverse': "\033[7m",
        'concealed': "\033[8m",

        'black': "\033[30m",
        'red': "\033[31m",
        'green': "\033[32m",
        'yellow': "\033[33m",
        'blue': "\033[34m",
        'magenta': "\033[35m",
        'cyan': "\033[36m",
        'white': "\033[37m",

        'on_black': "\033[40m",
        'on_red': "\033[41m",
        'on_green': "\033[42m",
        'on_yellow': "\033[43m",
        'on_blue': "\033[44m",
        'on_magenta': "\033[45m",
        'on_cyan': "\033[46m",
        'on_white': "\033[47m",

        'beep': "\007",
    }

    @classmethod
    def colorize(cls, s, color=None):
        """
        colorize string
        """
        if color in cls.colors:
            return "{0}{1}{2}".format(
                cls.colors[color], s, cls.colors['default'])
        else:
            return s


_color = Colorizing.colorize


class KafkaCli(object):
    """
    kafka cli
    """
    CMD_HELP_LINES = {
        "list": "list <optional: match pattern, regex format>",
        "partition": "partition <required: topic>"
    }
    CMD_OPTIONS = [
        "list",
        "partition",
    ]
    def __init__(self, server_addr):
        self.server_addr = server_addr

        self.consumer = None
        self.producer = None

        self.cmd_proc_funcs = {}
        self.prompt_line = _color("kafka> ", "cyan")
        # reg
        self.reg_all_cmds()

    def connect(self):
        """
        connect to kafka
        """
        try:
            self.consumer = KafkaConsumer(bootstrap_servers=self.server_addr)
            return True, "Success"
        except Exception as e:
            return False, "connect to {} failed, {}".format(self.server_addr, e)

    def cmd_completer(self, text, state):
        """
        cmd completer
        """
        # on first trigger, build possible matches
        matches = []
        if state == 0:
            # cache matches (entries that start with entered text)
            if text:
                matches = [s for s in self.CMD_OPTIONS 
                        if s and s.startswith(text)]
            else:  
                # no text entered, all matches possible
                matches = self.options[:]

        # return match indexed by state
        try:
            return matches[state]
        except IndexError:
            return None

    def prepare_auto_complete(self):
        """
        prepare auto complete
        """
        # cmd complete
        readline.set_completer(self.cmd_completer)
        readline.parse_and_bind('tab: complete')

    def reg_cmd_process(self, cmd_starts, func):
        """
        register cmd process function
        """
        self.cmd_proc_funcs[cmd_starts] = func

    def reg_all_cmds(self):
        """
        reg all cmds
        """
        # help
        self.reg_cmd_process("help", self.print_help)
        # list
        self.reg_cmd_process("list", self.list_topics)
        # partition offsets
        self.reg_cmd_process("partition", self.get_partitions)

    def dispatch_cmd(self, cmd_line):
        """
        dispatch
        """
        matches = [s for s in self.cmd_proc_funcs 
                if s and cmd_line.startswith(s)]
        # get the first 
        if matches:
            match_cmd = matches[0]
            self.cmd_proc_funcs[match_cmd](cmd_line)
        else:
            self.print_help()

    def print_help(self, cmd=None, cmd_line=None):
        """
        help
        """
        print("Usage: ")
        if not cmd or cmd not in self.CMD_HELP_LINES:
            for cmd in self.CMD_HELP_LINES:
                print("{}".format(self.CMD_HELP_LINES[cmd]))
                print("")
        else:
            print("{}\n".format(self.CMD_HELP_LINES[cmd]))

    def print_sep_line(self):
        """
        print seprate line
        """
        print(_color("+{}+".format("-" * 50), 'magenta'))

    def list_topics(self, cmd_line):
        """
        list topics
        """
        line_info = re.split("\s+", cmd_line)
        match_pattern = None
        if len(line_info) > 1:
            match_pattern = re.compile(line_info[1])

        topics = self.consumer.topics()
        if topics:
            cnt = 0
            print(_color("+{}+".format("-" * 50), 'magenta'))
            for topic in topics:
                if match_pattern:
                    m = match_pattern.match(topic)
                    if m:
                        print(topic)
                        cnt += 1
                else:
                    print(topic)
                    cnt += 1
            print(_color("+{}+".format("-" * 50), 'magenta'))
            print(_color("\nGet {} result(s)\n".format(cnt), 'yellow'))
        else:
            print(_color("\nGet 0 result(s)\n", 'yellow'))

    def get_partitions(self, cmd_line):
        """
        get topic partitions
        """
        line_info = re.split("\s+", cmd_line)
        if len(line_info) < 2:
            self.print_help(cmd="partition")
        else:
            topics = line_info[1:]
            offsets = {}
            for topic in topics:
                partition_ids = self.consumer.partitions_for_topic(topic)
                if not partition_ids:
                    continue
                offsets[topic] = {}
                topic_partitions = \
                    [TopicPartition(topic, p_id) for p_id in partition_ids]
                # begin offsets
                begin_offsets = self.consumer.beginning_offsets(topic_partitions)
                # end offsets
                end_offsets = self.consumer.end_offsets(topic_partitions)
                for tp in topic_partitions:
                    p_id = tp.partition
                    offsets[topic][p_id] = {}
                    offsets[topic][p_id]["begin"] = begin_offsets[tp]
                    offsets[topic][p_id]["end"] = end_offsets[tp]
            # print result
            for topic in topics:
                if topic in offsets:
                    self.print_sep_line()
                    print("{}".format(topic))
                    for p_id in offsets[topic]:
                        print("{} {}:{}".format(
                            _color(p_id, 'yellow'),
                            offsets[topic][p_id]["begin"],
                            offsets[topic][p_id]["end"]
                            ))
                else:
                    self.print_sep_line()
                    print("Get no partitions for topic {}".format(topic))
            self.print_sep_line()
            print("")

    def run(self):
        """
        run cli
        """
        if not self.consumer \
            and not self.connect():
            return False
        # cmd complete
        self.prepare_auto_complete()
        # loop
        while True:
            line = raw_input(self.prompt_line)
            line = line.strip()
            if not line:
                continue

            try:
                self.dispatch_cmd(line)
            except Exception as e:
                print("Exception occored, {}".format(e))

# cli 
DESCRIPTION = "Kafka CLI"
VERSION = "0.1"


def quit(signum, frame):
    """
    deal with interrupt signal
    """
    print('\nThanks for using, going to stop, bye bye...')
    sys.exit()


def usage():
    """
    Print The Usage of this Mini_Spider
    """
    print("Usage: python kafka_cli.py [-s <server_addr>]")
    print("Details: ")
    print("  <server_addr>  - kafka server address, ip:port")


def welcome():
    """
    welcome
    """
    welcome_line = ""
    welcome_line += "{}\n".format("#" * 50)
    welcome_line += '{} (Version {})\n'.format(DESCRIPTION, VERSION)
    welcome_line += 'Type "help", "?" for more information.\n'
    welcome_line += "{}\n".format("#" * 50)

    print(_color(welcome_line, 'yellow'))


def main():
    """
    main function
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], 
            "hs:", 
            ["help", "server="])
        if opts == []:
            usage()
            sys.exit()
        # params
        server_addr = None
        for op, value in opts: 
            if op in ("-s", "--server"):
                server_addr = value
            elif op in ("-h", "--help"):
                usage()
                sys.exit()
        # check
        if not server_addr:
            usage()
            sys.exit()
        # start
        kc = KafkaCli(server_addr=server_addr)
        ret_connect = kc.connect()
        if ret_connect[0]:
            # deal with ctrl+c, ctrl+z
            signal.signal(signal.SIGINT, quit)
            signal.signal(signal.SIGTERM, quit)
            # start
            welcome()
            kc.run()
        else:
            print("Oops, connecting to server {} failed.".format(server_addr))
            print("Reason: {}".format(ret_connect[1]))
    except Exception as e:
        usage()
        print(e)
        sys.exit()


if __name__ == '__main__':
    main()
