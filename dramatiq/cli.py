# This file is a part of Dramatiq.
#
# Copyright (C) 2017,2018,2019 CLEARTYPE SRL <bogdan@cleartype.io>
#
# Dramatiq is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# Dramatiq is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Don't depend on *anything* in this module.  The contents of this
# module can and *will* change without notice.

import argparse
import atexit
import functools
import importlib
import logging
import multiprocessing
import os
import random
import signal
import sys
import time
from itertools import chain
from threading import Thread

from dramatiq import Broker, ConnectionError, Worker, __version__, get_broker, get_logger
from dramatiq.canteen import Canteen, canteen_add, canteen_get
from dramatiq.compat import StreamablePipe, file_or_stderr

try:
    from .watcher import setup_file_watcher

    HAS_WATCHDOG = True
except ImportError:  # pragma: no cover
    HAS_WATCHDOG = False

#: The exit codes that the master process returns.
RET_OK = 0  # The process terminated successfully.
RET_KILLED = 1  # The process was killed.
RET_IMPORT = 2  # Module import(s) failed or invalid command line argument.
RET_CONNECT = 3  # Broker connection failed during worker startup.
RET_PIDFILE = 4  # PID file points to an existing process or cannot be written to.
RET_RESTART = 253

#: The size of the logging buffer.
BUFSIZE = 65536

#: The number of available cpus.
CPUS = multiprocessing.cpu_count()

#: The logging format.
LOGFORMAT = "[%(asctime)s] [PID %(process)d] [%(threadName)s] [%(name)s] [%(levelname)s] %(message)s"

#: The logging verbosity levels.
VERBOSITY = {
    0: logging.INFO,
    1: logging.DEBUG,
}

#: Message printed after the help text.
HELP_EPILOG = """\
examples:
  # Run dramatiq workers with actors defined in `./some_module.py`.
  $ dramatiq some_module

  # Run with a broker named "redis_broker" defined in "some_module".
  $ dramatiq some_module:redis_broker

  # Run with a broker named "broker" defined as attribute of "app" in "some_module".
  $ dramatiq some_module:app.broker

  # Auto-reload dramatiq when files in the current directory change.
  $ dramatiq --watch . some_module

  # Run dramatiq with 1 thread per process.
  $ dramatiq --threads 1 some_module

  # Run dramatiq with gevent.  Make sure you `pip install gevent` first.
  $ dramatiq-gevent --processes 1 --threads 1024 some_module

  # Import extra modules.  Useful when your main module doesn't import
  # all the modules you need.
  $ dramatiq some_module some_other_module

  # Listen only to the "foo" and "bar" queues.
  $ dramatiq some_module -Q foo bar

  # Write the main process pid to a file.
  $ dramatiq some_module --pid-file /tmp/dramatiq.pid

  # Write logs to a file.
  $ dramatiq some_module --log-file /tmp/dramatiq.log
"""


def import_object(value):
    modname, varname = value, None
    if ":" in value:
        modname, varname = value.split(":", 1)

    module = importlib.import_module(modname)
    if varname is not None:
        varnames = varname.split(".")
        try:
            return module, functools.reduce(getattr, varnames, module)
        except AttributeError:
            raise ImportError("Module %r does not define a %r variable." % (modname, varname))
    return module, None


def import_broker(value):
    module, broker = import_object(value)
    if broker is None:
        return module, get_broker()

    if not isinstance(broker, Broker):
        raise ImportError("%r is not a Broker." % value)
    return module, broker


def folder_path(value):
    if not os.path.isdir(value):
        raise argparse.ArgumentError("%r is not a valid directory" % value)
    return os.path.abspath(value)


def make_argument_parser():
    parser = argparse.ArgumentParser(
        prog="dramatiq",
        description="Run dramatiq workers.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=HELP_EPILOG,
    )
    parser.add_argument(
        "broker",
        help="the broker to use (eg: 'module' or 'module:a_broker')",
    )
    parser.add_argument(
        "modules", metavar="module", nargs="*",
        help="additional python modules to import",
    )
    parser.add_argument(
        "--processes", "-p", default=CPUS, type=int,
        help="the number of worker processes to run (default: %s)" % CPUS,
    )
    parser.add_argument(
        "--threads", "-t", default=8, type=int,
        help="the number of worker threads per process (default: 8)",
    )
    parser.add_argument(
        "--path", "-P", default=".", nargs="*", type=str,
        help="the module import path (default: .)"
    )
    parser.add_argument(
        "--queues", "-Q", nargs="*", type=str,
        help="listen to a subset of queues (default: all queues)",
    )
    parser.add_argument(
        "--pid-file", type=str,
        help="write the PID of the master process to a file (default: no pid file)",
    )
    parser.add_argument(
        "--log-file", type=str,
        help="write all logs to a file (default: sys.stderr)",
    )
    parser.add_argument(
        "--use-spawn", action="store_true",
        help="start processes by spawning (default: fork on unix, spawn on windows)"
    )
    parser.add_argument(
        "--fork-function", "-f", action="append", dest="forks", default=[],
        help="fork a subprocess to run the given function"
    )

    if HAS_WATCHDOG:
        parser.add_argument(
            "--watch", type=folder_path, metavar="DIR",
            help=(
                "watch a directory and reload the workers when any source files "
                "change (this feature must only be used during development)"
            )
        )
        parser.add_argument(
            "--watch-use-polling",
            action="store_true",
            help=(
                "poll the filesystem for changes rather than using a "
                "system-dependent filesystem event emitter"
            )
        )

    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--verbose", "-v", action="count", default=0, help="turn on verbose log output")
    return parser


def setup_pidfile(filename):
    try:
        pid = os.getpid()
        with open(filename, "r") as pid_file:
            old_pid = int(pid_file.read().strip())
            # This can happen when reloading the process via SIGHUP.
            if old_pid == pid:
                return pid

        try:
            os.kill(old_pid, 0)
            raise RuntimeError("Dramatiq is already running with PID %d." % old_pid)
        except OSError:
            try:
                os.remove(filename)
            except FileNotFoundError:
                pass

    except FileNotFoundError:  # pragma: no cover
        pass

    except ValueError:
        # Abort here to avoid overwriting real files.  Eg. someone
        # accidentally specifies a config file as the pid file.
        raise RuntimeError("PID file contains garbage. Aborting.")

    try:
        with open(filename, "w") as pid_file:
            pid_file.write(str(pid))

        # Change permissions to -rw-r--r--.
        os.chmod(filename, 0o644)
        return pid
    except (FileNotFoundError, PermissionError) as e:
        raise RuntimeError("Failed to write PID file %r. %s." % (e.filename, e.strerror))


def remove_pidfile(filename, logger):
    try:
        logger.debug("Removing PID file %r.", filename)
        os.remove(filename)
    except FileNotFoundError:  # pragma: no cover
        logger.debug("Failed to remove PID file. It's gone.")


def setup_parent_logging(args, *, stream=sys.stderr):
    level = VERBOSITY.get(args.verbose, logging.DEBUG)
    logging.basicConfig(level=level, format=LOGFORMAT, stream=stream)
    return get_logger("dramatiq", "MainProcess")


def make_logging_setup(prefix):
    def setup_logging(args, child_id, logging_pipe):
        # Redirect all output to the logging pipe so that all output goes
        # to stderr and output is serialized so there isn't any mangling.
        sys.stdout = logging_pipe
        sys.stderr = logging_pipe

        level = VERBOSITY.get(args.verbose, logging.DEBUG)
        logging.basicConfig(level=level, format=LOGFORMAT, stream=logging_pipe)
        logging.getLogger("pika").setLevel(logging.CRITICAL)
        return get_logger("dramatiq", "%s(%s)" % (prefix, child_id))

    return setup_logging


setup_worker_logging = make_logging_setup("WorkerProcess")
setup_fork_logging = make_logging_setup("ForkProcess")


def watch_logs(log_filename, pipes):
    with file_or_stderr(log_filename, mode="a", encoding="utf-8") as log_file:
        while pipes:
            try:
                events = multiprocessing.connection.wait(pipes, timeout=1)
                for event in events:
                    try:
                        while event.poll():
                            # StreamHandler writes newlines into the pipe separately
                            # from the actual log entry; to avoid back-to-back newlines
                            # in the events pipe (causing multiple entries on a single
                            # line), discard newline-only data from the pipe
                            try:
                                data = event.recv_bytes()
                            except EOFError:
                                event.close()
                                raise

                            data = data.decode("utf-8", errors="replace").rstrip("\n")
                            if not data:
                                break

                            log_file.write(data + "\n")
                            log_file.flush()
                    except BrokenPipeError:
                        event.close()
                        raise
            # If one of the worker processes is killed, its handle will be
            # closed so waiting for it is going to fail with this OSError.
            # Additionally, event.recv() raises EOFError when its pipe
            # is closed, and event.poll() raises BrokenPipeError when
            # its pipe is closed.  When any of these events happen, we
            # just take the closed pipes out of the waitlist.
            except (BrokenPipeError, EOFError, OSError):
                pipes = [p for p in pipes if not p.closed]


def worker_process(args, worker_id, logging_pipe, canteen):
    try:
        # Re-seed the random number generator from urandom on
        # supported platforms.  This should make it so that worker
        # processes don't all follow the same sequence.
        random.seed()

        logger = setup_worker_logging(args, worker_id, logging_pipe)
        logger.debug("Loading broker...")
        module, broker = import_broker(args.broker)
        broker.emit_after("process_boot")

        logger.debug("Loading modules...")
        for module in args.modules:
            importlib.import_module(module)

        if not canteen.initialized:
            with canteen.get_lock():
                if not canteen.initialized:
                    logger.debug("Sending forks to main process...")
                    for middleware in broker.middleware:
                        for fork in middleware.forks:
                            fork_path = "%s:%s" % (fork.__module__, fork.__name__)
                            canteen_add(canteen, fork_path)

        logger.debug("Starting worker threads...")
        worker = Worker(broker, queues=args.queues, worker_threads=args.threads)
        worker.start()
    except ImportError:
        logger.exception("Failed to import module.")
        return sys.exit(RET_IMPORT)
    except ConnectionError:
        logger.exception("Broker connection failed.")
        return sys.exit(RET_CONNECT)

    def termhandler(signum, frame):
        nonlocal running
        if running:
            logger.info("Stopping worker process...")
            running = False
        else:
            logger.warning("Killing worker process...")
            return sys.exit(RET_KILLED)

    logger.info("Worker process is ready for action.")
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, termhandler)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, termhandler)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, termhandler)

    running = True
    while running and not worker.restart_requested:
        time.sleep(1)

    if worker.restart_requested:
        logger.info("Requesting worker restart.")
    worker.stop()
    logger.info("Worker stopped.")
    broker.close()

    logger.info('restart_requested=%s' % worker.restart_requested)
    print('restart_requested=%s' % worker.restart_requested)

    if worker.restart_requested:
        logger.debug("Terminating worker process with code %r.", RET_RESTART)
        logging_pipe.close()
        os._exit(RET_RESTART)
    logging_pipe.close()


def fork_process(args, fork_id, fork_path, logging_pipe):
    try:
        # Re-seed the random number generator from urandom on
        # supported platforms.  This should make it so that worker
        # processes don't all follow the same sequence.
        random.seed()

        logger = setup_fork_logging(args, fork_id, logging_pipe)
        logger.debug("Loading fork function...")

        _, func = import_object(fork_path)
    except ImportError:
        logger.exception("Failed to import module.")
        return sys.exit(RET_IMPORT)

    stopped = False

    def termhandler(signum, frame):
        nonlocal stopped
        if stopped:
            logger.warning("Killing fork process...")
            return sys.exit(RET_KILLED)
        else:
            logger.info("Stopping fork process...")
            stopped = True
            return sys.exit(RET_OK)

    logger.info("Fork process %r is ready for action.", fork_path)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, termhandler)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, termhandler)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, termhandler)

    return sys.exit(func())


def main(args=None):  # noqa
    args = args or make_argument_parser().parse_args()
    for path in args.path:
        sys.path.insert(0, path)

    if args.use_spawn:
        multiprocessing.set_start_method("spawn")

    try:
        if args.pid_file:
            setup_pidfile(args.pid_file)
    except RuntimeError as e:
        with file_or_stderr(args.log_file) as stream:
            logger = setup_parent_logging(args, stream=stream)
            logger.critical(e)
            return RET_PIDFILE

    canteen = multiprocessing.Value(Canteen)
    worker_pipes = []
    worker_write_pipes = []
    worker_processes = []
    pid_to_worker_id = {}

    def create_worker_proc(worker_id, write_pipe):
        proc = multiprocessing.Process(
            target=worker_process,
            args=(args, worker_id, StreamablePipe(write_pipe), canteen),
            daemon=True,
        )
        return proc

    for worker_id in range(args.processes):
        read_pipe, write_pipe = multiprocessing.Pipe()
        proc = create_worker_proc(worker_id, write_pipe)
        proc.start()
        worker_pipes.append(read_pipe)
        worker_write_pipes.append(write_pipe)
        worker_processes.append(proc)
        pid_to_worker_id[proc.pid] = worker_id

    fork_pipes = []
    fork_processes = []
    for fork_id, fork_path in enumerate(chain(args.forks, canteen_get(canteen))):
        read_pipe, write_pipe = multiprocessing.Pipe()
        proc = multiprocessing.Process(
            target=fork_process,
            args=(args, fork_id, fork_path, StreamablePipe(write_pipe)),
            daemon=True,
        )
        proc.start()
        fork_pipes.append(read_pipe)
        fork_processes.append(proc)

    parent_read_pipe, parent_write_pipe = multiprocessing.Pipe()
    logger = setup_parent_logging(args, stream=StreamablePipe(parent_write_pipe))
    logger.info("Dramatiq %r is booting up." % __version__)
    if args.pid_file:
        atexit.register(remove_pidfile, args.pid_file, logger)

    running, reload_process = True, False

    # To avoid issues with signal delivery to user threads on
    # platforms such as FreeBSD 10.3, we make the main thread block
    # the signals it expects to handle before spawning the file
    # watcher and log watcher threads so that those threads can
    # inherit the blocking behaviour.
    if hasattr(signal, "pthread_sigmask"):
        signal.pthread_sigmask(
            signal.SIG_BLOCK,
            {signal.SIGINT, signal.SIGTERM, signal.SIGHUP},
        )

    if HAS_WATCHDOG and args.watch:
        file_watcher = setup_file_watcher(args.watch, args.watch_use_polling)

    log_watcher = Thread(
        target=watch_logs,
        args=(args.log_file, [parent_read_pipe, *worker_pipes, *fork_pipes]),
        daemon=False,
    )
    log_watcher.start()

    def stop_subprocesses(signum):
        nonlocal running
        running = False

        for proc in chain(worker_processes, fork_processes):
            try:
                os.kill(proc.pid, signum)
            except OSError:  # pragma: no cover
                if proc.exitcode is None:
                    logger.warning("Failed to send %r to PID %d.", signum.name, proc.pid)

    def sighandler(signum, frame):
        nonlocal reload_process
        reload_process = signum == getattr(signal, "SIGHUP", None)
        if signum == signal.SIGINT:
            signum = signal.SIGTERM

        logger.info("Sending signal %r to subprocesses...", getattr(signum, "name", signum))
        stop_subprocesses(signum)

    # Now that the watcher threads have been started, it should be
    # safe to unblock the signals that were previously blocked.
    if hasattr(signal, "pthread_sigmask"):
        signal.pthread_sigmask(
            signal.SIG_UNBLOCK,
            {signal.SIGINT, signal.SIGTERM, signal.SIGHUP},
        )

    retcode = RET_OK
    signal.signal(signal.SIGINT, sighandler)
    signal.signal(signal.SIGTERM, sighandler)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, sighandler)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, sighandler)

    # Wait for all workers to terminate.  If any of the processes
    # terminates unexpectedly, then shut down the rest as well.
    while any(p.exitcode in (None, RET_RESTART) for p in worker_processes):
        for proc in list(worker_processes):
            proc.join(timeout=1)
            logger.debug("Waiting worker with PID %r.", proc.pid)
            if proc.exitcode is None:
                logger.debug("Waiting with PID %r is running.", proc.pid)
                continue
            logger.debug("Worker PID %r finished (code %r).", proc.pid, proc.exitcode)

            if proc.exitcode == RET_RESTART and running:
                logger.debug("Worker with PID %r ask for restart (code %r).", proc.pid, proc.exitcode)
                worker_id = pid_to_worker_id[proc.pid]
                write_pipe = worker_write_pipes[worker_id]

                proc = create_worker_proc(worker_id, write_pipe)
                proc.start()
                worker_processes[worker_id] = proc
                pid_to_worker_id[proc.pid] = worker_id
                continue

            if running:  # pragma: no cover
                logger.critical("Worker with PID %r exited unexpectedly (code %r). Shutting down...", proc.pid, proc.exitcode)
                stop_subprocesses(signal.SIGTERM)
                retcode = proc.exitcode
                break

            else:
                retcode = max(retcode, proc.exitcode)

    print('Loop exit')
    for proc in list(worker_processes):
        print('Worker with PID %r exited with code %r' % (proc.pid, proc.exitcode))

    for pipe in [parent_read_pipe, parent_write_pipe, *worker_pipes, *fork_pipes]:
        pipe.close()

    # The log watcher can't be a daemon in case we log to a file.  So
    # we have to wait for it to complete on exit.  Closing all the
    # pipes above is what should trigger said exit.
    log_watcher.join()

    if HAS_WATCHDOG and args.watch:
        file_watcher.stop()
        file_watcher.join()

    if reload_process:
        if sys.argv[0].endswith("/dramatiq/__main__.py"):
            return os.execvp(sys.executable, ["python", "-m", "dramatiq", *sys.argv[1:]])
        return os.execvp(sys.argv[0], sys.argv)

    return retcode
