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

import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable, List, Optional

from django.db import transaction

from aap_eda.core.models import ActivationInstance, ActivationInstanceEvent
from aap_eda.services import event_consumer as consumer

NLREGEX = re.compile(r"\r\n?|\n")
DEFAULT_CHUNK_SIZE = consumer.DEFAULT_CHUNK_SIZE


@dataclass
class LogStruct:
    event_text: str
    text_line_count: int
    event_raw: Any


class EDALogConsumer(consumer.EventConsumer):
    def __init__(
        self,
        /,
        activation_instance: ActivationInstance,
        event_reader: Iterable,
        event_forwarder: Optional[Callable] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        super().__init__(
            event_writer=self.log_event_writer,
            event_reader=event_reader,
            event_parser=self._parser,
            event_forwarder=event_forwarder,
            chunk_size=chunk_size,
            extra={"activation_instance": activation_instance},
        )

    def _parser(self, raw_log) -> LogStruct:
        if isinstance(raw_log, bytes):
            log_text = raw_log.decode()
        else:
            log_text = raw_log
        line_count = count_lines_in_text(log_text)
        return LogStruct(log_text, line_count, raw_log)

    @transaction.atomic
    def log_event_writer(
        self,
        chunk_meta: consumer.EventConsumer.ChunkMeta,
        chunk_data: List[LogStruct],
    ):
        ActivationInstanceEvent.objects.create(
            activation_id=chunk_meta.extra[
                "activation_instance"
            ].activation_id,
            activation_instance_id=chunk_meta.extra["activation_instance"].id,
            created_at=consumer.utcnow(),
            event_number=chunk_meta.chunk_number,
            event_chunk_start_line=chunk_meta.stream_start_line,
            event_chunk_end_line=chunk_meta.stream_end_line,
            event_chunk="".join(c.event_text for c in chunk_data),
        )


def count_lines_in_text(text_buff: str) -> int:
    bufflen = len(text_buff)
    nlfound = list(NLREGEX.finditer(text_buff))
    if nlfound:
        num_lines = len(nlfound) + int(nlfound[-1].end() < bufflen)
    else:
        num_lines = int(bufflen > 0)

    return num_lines
