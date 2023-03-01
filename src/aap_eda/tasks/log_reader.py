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
from subprocess import Popen
from typing import Callable, Optional

from docker import DockerClient
from docker.models.containers import Container

from aap_eda.core.models import ActivationInstance
from aap_eda.core.tasking import job
from aap_eda.services.eda_consumer import DEFAULT_CHUNK_SIZE, EDALogConsumer
from aap_eda.settings.default import AnsibleRunnerDeployment

LOG = logging.getLogger(__name__)


class ActivationExecution:
    def __init__(
        self,
        deployment_type: AnsibleRunnerDeployment,
        *,
        subprocess: Optional[Popen] = None,
        docker_client: Optional[DockerClient] = None,
        container: Optional[Container] = None,
        consumer: Optional[EDALogConsumer] = None,
    ):
        self.deployment_type = AnsibleRunnerDeployment(deployment_type)
        self.subprocess = subprocess
        self.docker_client = docker_client
        self.container = container
        self.consumer = consumer

        self.validate()

    def validate(self):
        if self.deployment_type == AnsibleRunnerDeployment.LOCAL:
            if self.subprocess is None:
                raise ValueError("subprocess is required for local deployment")
        elif self.deployment_type == AnsibleRunnerDeployment.DOCKER:
            if self.docker_client is None:
                raise ValueError(
                    "docker_client is required for docker deployment"
                )
            if self.container is None:
                raise ValueError("container is required for docker deployment")


# Standard read iterator for subprocesses
def read_subproc_output(subproc: Popen):
    for line in subproc.stdout:
        if not line:
            break
        if isinstance(line, bytes):
            line = line.decode()
        yield line


# Standard read iterator for docker container logs
def read_container_output(
    docker_client: DockerClient,
    container: Container,
):
    try:
        for line in container.logs(stdout=True, stderr=True, stream=True):
            if isinstance(line, bytes):
                line = line.decode()
            yield line
    except Exception as e:
        LOG.error(f"read_container_output: {type(e)}: {e}")
    finally:
        docker_client.close()


def get_consumer(
    execution: ActivationExecution,
    activation_instance: ActivationInstance,
    forwarder: Optional[Callable] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> EDALogConsumer:
    if execution.deployment_type == AnsibleRunnerDeployment.LOCAL:
        reader = iter(read_subproc_output(execution.subprocess))
    else:
        reader = iter(
            read_container_output(execution.docker_client, execution.container)
        )

    execution.consumer = EDALogConsumer(
        activation_instance=activation_instance,
        event_reader=reader,
        event_forwarder=forwarder,
        chunk_size=chunk_size,
    )

    return execution.consumer


@job
def consume_log(
    execution: ActivationExecution,
    activation_instance: ActivationInstance,
    forwarder: Optional[Callable] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
):
    consumer = get_consumer(
        execution,
        activation_instance,
        forwarder=forwarder,
        chunk_size=chunk_size,
    )

    consumer.consume()
