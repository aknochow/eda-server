#  Copyright 2023 Red Hat, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import os
import uuid
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Callable, Iterable, Optional

LOG = logging.getLogger(__name__)


DEFAULT_CHUNK_SIZE = 4000


class EventConsumer:
    """This class is the consumer.

    It will chunk events together before calling a
    supplied writer to write the data.
    This was done to try to accomodate usage in multiple projects.
    This could be subclassed to be a consumer for a specific project.

    Required:
    event_writer   : Callable - Write chunks to DB.
                                Parameters:
                                    chunk_meta: EventConsumer.ChunkData,
                                    chunk_data: Any,
    event_reader   : Iterable - Iterable from which events will be received.
    event_parser   : Callable - Receive raw event, parse into a structure that
                                can be used by the writer and forwarder.
                                The text of the event should be collected and
                                Parsed as full lines if possible.
                                Parameters:
                                event: Any,

    Optional:
    event_forwarder: Callable - Function to forward the event to other
                                processing code (such as websocket
                                transmission). This data will be the output
                                from the parser function.
                                Parameters:
                                    event_data: Any (output from parser),
    chunk_size     : int      - Number of bytes that should make up a chunk.
                                Once enough lines are read (or StopIteration)
                                The chunk will be written to the database.
    extra          : dict     - Extra information that will be passed to
                                each instantiation of ChunkMeta to make it
                                available to event_writer calls.
    """  # noqa: D208

    # Class specific to the consumer.
    # It will track this data and pass it to the writer.
    class ChunkMeta:
        """ChunkMeta holds metadata regarding the chunk of output data.

        Fields:
            chunk_created     : timestamp of the chunk creation
            chunk_number      : oridnal of chunks
            stream_start_line : Starting line number of the chunked data
            stream_end_line   : Ending line number of the chunked data
            extra             : extra data from the consumer class
        """  # noqa: D208

        def __init__(
            self,
            chunk_number: int,
            start_line: int,
            end_line: int,
            created: Optional[datetime] = None,
            extra: Optional[Any] = None,
        ):
            self.chunk_created = utcnow() if not created else created
            self.chunk_number = chunk_number
            self.stream_start_line = start_line
            self.stream_end_line = end_line
            self.extra = extra

        def __repr__(self):
            return (
                f"{type(self)}(chunk_created={self.chunk_created}, "
                f"chunk_number={self.chunk_number}, "
                f"start_line={self.stream_start_line}, "
                f"end_line={self.stream_end_line}, "
                f"extra={self.extra}"
            )

        def __str__(self):
            return repr(self)

    # End ChunkMeta

    INITIALIZED = "initialized"
    CONSUMING = "consuming"
    COMPLETED = "completed"
    INTERRUPT = "interrupt"

    def __init__(
        self,
        /,
        event_writer: Callable,
        event_reader: Iterable,
        event_parser: Callable,
        event_forwarder: Optional[Callable] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        extra: Optional[Any] = None,
    ):
        self.__id = uuid.uuid4()

        self.writer = event_writer
        self.reader = event_reader
        self.parser = event_parser
        self.forwarder = event_forwarder
        self.chunk_size = chunk_size
        self.extra = extra

        self.__consumed_events = 0
        self.__chunks_written = 0
        self.__status_lock = Lock()
        self.__set_status(self.INITIALIZED)

        self.__log_init()

    def __repr__(self):
        return f"<{self.__class__.__name__}> id = {self.id} status = {self.status}"  # noqa: E501

    def __str__(self):
        return repr(self)

    @property
    def id(self):
        return self.__id

    @property
    def consumed_events(self):
        return self.__consumed_events

    @property
    def chunks_written(self):
        return self.__chunks_written

    @property
    def status(self):
        with self.__status_lock:
            return self.__status

    def __log_init(self):
        fwd_name = (
            "None" if self.forwarder is None else self.forwarder.__name__
        )
        dbg_msg_parts = [
            f"    instance id : {self.id}",
            f"    writer      : {self.writer.__name__}",
            f"    reader      : {self.reader.__name__}",
            f"    parser      : {self.parser.__name__}",
            f"    forwarder   : {fwd_name}",
            f"    chunk_size  : {self.chunk_size}",
            f"    extra       : {self.extra}",
        ]
        LOG.debug(
            f"{self.__class__.__name__} instantiated with:"
            f"{os.linesep.join(dbg_msg_parts)}"
        )

    def consume(self):
        """Consume output from reader.

        As data are consumed from the reader, it is gathered together until
        the total byte size is >= chunk_size. Once that threashold is reached,
        the writer is called.
        For each datum consumed, the parser is called and the resulting class
        instance is passed to the forwarder (if supplied).
        """
        self.__set_status(self.CONSUMING)
        chunk_data = []
        chunk_len = 0
        start_line = 1
        end_line = 0

        LOG.info(f"{self.id}:: Consuming output from {self.reader.__name__}")

        try:
            for event in self.reader:
                self.__consumed_events += 1
                event_data = self.parser(event)
                chunk_len += len(event_data.event_text)
                end_line += event_data.text_line_count
                chunk_data.append(event_data)

                if chunk_len > self.chunk_size:
                    self.__chunks_written += 1
                    chunk_meta = self.ChunkMeta(
                        self.__chunks_written,
                        start_line,
                        end_line,
                        extra=self.extra,
                    )
                    LOG.debug(
                        f"{self.id}:: Calling writer for chunk number "
                        + str(self.__chunks_written)
                    )
                    self.writer(chunk_meta, chunk_data)
                    chunk_meta = None
                    chunk_data = []
                    chunk_len = 0
                    start_line = end_line + 1

                if self.forwarder:
                    self.forwarder(event_data)

                if self.status == self.INTERRUPT:
                    break
        finally:
            if chunk_data:
                self.__chunks_written += 1
                chunk_meta = self.ChunkMeta(
                    self.__chunks_written,
                    start_line,
                    end_line,
                    extra=self.extra,
                )
                LOG.debug(
                    f"{self.id}:: Final call to writer for chunk number "
                    + str(self.__chunks_written)
                )
                self.writer(chunk_meta, chunk_data)
                self.__set_status(self.COMPLETED)

    def interrupt(self):
        """Set interrupt status to stop consumption."""
        if self.status == self.CONSUMING:
            LOG.info(f"{self.id}:: INTERRUPTING OUTPUT CONSUMPTION")
            self.__set_status(self.INTERRUPT)

    def __set_status(self, status: str):
        with self.__status_lock:
            self.__status = status


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)
