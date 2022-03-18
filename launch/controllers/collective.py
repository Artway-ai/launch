# Copyright 2022 kuizhiqing
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .controller import Controller

from ..context.constants import ENV_PREFIX

import json
import os
import six
import time


class CollectiveController(Controller):
    @classmethod
    def enable(cls, ctx):
        if ctx:
            ctx.logger.debug("{} enabled".format(cls.__name__))
            return True
        else:
            return False

    def build_pod(self):
        self.pod.replicas = self.pod_replicas()

        # rank will be reset when restart
        self.pod.rank = self.ctx.args.rank

        port = self.ctx.node.get_free_port()

        # compatible
        endpoints = [
            "{}:{}".format(self.ctx.node.ip, p)
            for p in self.ctx.node.get_free_ports(self.pod.replicas)
        ]

        data = json.dumps({
            'name': self.pod.name,
            'rank': self.pod.rank,
            'replicas': self.pod.replicas,
            'dtype': self.ctx.node.device.dtype,
            'candidate': '{}:{}'.format(self.ctx.node.ip, port),
            'endpoints': ",".join(endpoints),
        })

        peer_list, rank = self.master.sync_peers(
            '/{}/info'.format(self.job.id), self.pod.name, data,
            self.job.replicas, self.pod.rank)
        self.pod.rank = rank

        if len(peer_list) < 1:
            return False

        peer_list = [json.loads(i) for i in peer_list]

        self.ctx.logger.debug("sync peers done {}".format(peer_list))
        self.save_pod_log(peer_list)

        global_size = sum([i['replicas'] for i in peer_list])
        rank_offset = sum([i['replicas'] for i in peer_list[:rank]])
        '''
        The new designed collective need nothing but a master endpoint
        '''
        collective_master = peer_list[0]['candidate']

        job_endpoints = [i['endpoints'] for i in peer_list]

        self.pod.reset()
        for i in range(self.pod.replicas):
            e = {
                f"{ENV_PREFIX}MASTER": collective_master,
                f"{ENV_PREFIX}GLOBAL_SIZE": "{}".format(global_size),
                f"{ENV_PREFIX}LOCAL_SIZE": "{}".format(self.pod.replicas),
                f"{ENV_PREFIX}GLOBAL_RANK": "{}".format(i + rank_offset),
                f"{ENV_PREFIX}LOCAL_RANK": "{}".format(i),
                f"{ENV_PREFIX}TRAINER_ENDPOINTS": ",".join(job_endpoints),
                f"{ENV_PREFIX}CURRENT_ENDPOINT": endpoints[i],
            }
            if self.pod.replicas == 1:
                e.update(self.ctx.node.device.selected_flags())
            else:
                e.update(self.ctx.node.device.selected_flags(i))
            self.add_container(envs=e, log_tag=i)

        return True


class CollectiveElasticController(CollectiveController):
    @classmethod
    def enable(cls, ctx):
        if ctx.args.master and ctx.args.master.startswith("etcd://"):
            ctx.logger.debug("{} enabled".format(cls.__name__))
            return True
        else:
            return False

    def register(self):
        if self.job.id == 'default':
            self.ctx.logger.warning(
                'Using default job name may cause conflict, add --job_id in args'
            )

        self.master.register_heartbeat(self.job.id, self.pod.name)

    def watch(self) -> bool:
        '''
        watch self and peer status, return true to exit
        '''

        self.ctx.logger.info("Watching {}".format(self.pod))
        while not self.ctx.status.is_done():
            # self status
            status = self.pod.watch(timeout=2)
            self.ctx.logger.debug("Pod status {}, Ctx status {}".format(
                status, self.ctx.status.current()))

            # completed
            if status == self.ctx.status.COMPLETED:
                self.master.set_status(status)
                self.ctx.status.complete()
                self.ctx.logger.info("Pod complete {}".format(status))
                return True

            # self failure
            elif status == self.ctx.status.FAILED:
                self.master.set_status(status)
                self.master.restart_peer()
                self.ctx.logger.info("Pod failed {}".format(status))
                self.pod.stop()

                if self.ctx.args.elastic_level <= 0:
                    return True
                else:
                    return False

            # peer failure
            if self.ctx.status.is_restarting() and self.master.get_status(
            ) != self.ctx.status.COMPLETED:
                self.pod.stop()
                return False

            #peers = self.master.fetch_peer_alive()
            #print("peers {}".format(peers))

    def run(self):

        timeout = self.ctx.args.elastic_timeout if self.job.elastic else self.ctx.args.elastic_timeout * 10
        self.register()

        while self.pod.restart <= self.ctx.args.max_restart:

            self.build_job()

            ok, replicas = self.master.wait_peer_ready(
                self.job.replicas_min, self.job.replicas_max, timeout)
            if ok:
                self.job.replicas = replicas
            else:
                self.ctx.logger.warnning("peer not ready {}".format(self.job))
                break

            self.ctx.logger.debug("Run {}".format(self.job))

            if not self.build_pod():
                continue

            self.master.set_status(self.ctx.status.RUNNING)

            self.deploy_pod()

            if self.watch():
                break

        self.ctx.logger.debug("Job done {}".format(self.job))
