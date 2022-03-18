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

from .controller import Controller, ControleMode

import json
import os, shutil

from ..context.constants import ENV_PREFIX

class PSController(Controller):
    @classmethod
    def enable(cls, ctx):
        if ctx.args.mode == ControleMode.PS or ctx.args.server_num or len(
                ctx.args.servers) > 0 or ctx.args.trainer_num or len(
                    ctx.args.trainers) > 0:
            ctx.logger.debug("{} enabled".format(cls.__name__))
            ctx.args.mode = ControleMode.PS
            return True
        else:
            return False

    def build_pod(self):
        if self.ctx.args.servers and self.ctx.args.trainers:
            self._build_pod_with_args()
        else:
            self._build_pod_with_master()

    def _build_pod_with_args(self):
        if '127.0.0.1' in self.ctx.args.servers:
            host = '127.0.0.1'
        else:
            host = self.ctx.node.ip

        server_endpoints = [s for s in self.ctx.args.servers.split(",")]
        trainer_endpoints = [s for s in self.ctx.args.trainers.split(",")]
        servers = [
            s for s in self.ctx.args.servers.split(",") if s.startswith(host)
        ]
        trainers = [
            s for s in self.ctx.args.trainers.split(",") if s.startswith(host)
        ]
        server_num = len(servers)
        trainer_num = len(trainers)

        self.pod.replicas = server_num + trainer_num

        self.save_pod_log([server_endpoints, trainer_endpoints])

        for i in range(server_num):
            e = {
                f"{ENV_PREFIX}PSERVERS_IP_PORT_LIST": self.ctx.args.servers,
                f"{ENV_PREFIX}TRAINER_ENDPOINTS": self.ctx.args.trainers,
                f"{ENV_PREFIX}PORT": servers[i].split(":")[1],
                f"{ENV_PREFIX}ROLE": "PSERVER",
                "POD_IP": self.ctx.node.ip,
            }
            log_tag = "ps.{}".format(i)
            self.add_container(envs=e, log_tag=log_tag)

        trainer_rank_offset = 0
        for s in trainer_endpoints:
            if s.startswith(host):
                break
            else:
                trainer_rank_offset += 1

        for i in range(trainer_num):
            e = {
                f"{ENV_PREFIX}PSERVERS_IP_PORT_LIST": self.ctx.args.servers,
                f"{ENV_PREFIX}TRAINER_ENDPOINTS": self.ctx.args.trainers,
                f"{ENV_PREFIX}PORT": servers[i].split(":")[1],
                f"{ENV_PREFIX}ROLE": "TRAINER",
                "POD_IP": self.ctx.node.ip,
            }
            log_tag = "trainer.{}".format(i)
            self.add_container(envs=e, log_tag=log_tag)

    def _build_pod_with_master(self):

        self.pod.rank = self.ctx.args.rank

        server_num = self.ctx.args.server_num or 1
        servers = [
            "{}:{}".format(self.ctx.node.ip, p)
            for p in self.ctx.node.get_free_ports(server_num)
        ]
        trainer_num = self.ctx.args.trainer_num or 1
        trainers = [
            "{}:{}".format(self.ctx.node.ip, p)
            for p in self.ctx.node.get_free_ports(trainer_num)
        ]

        data = json.dumps({
            'name': self.pod.name,
            'rank': self.pod.rank,
            'servers': servers,
            'trainers': trainers,
            'dtype': self.ctx.node.device.dtype,
        })

        peer_list, rank = self.master.sync_peers(
            '/{}/info'.format(self.job.id), self.pod.name, data,
            self.job.replicas, self.pod.rank)

        self.ctx.logger.debug("sync peers done {}".format(peer_list))

        peer_list = [json.loads(i) for i in peer_list]

        self.save_pod_log(peer_list)

        server_endpoints = [j for i in peer_list for j in i['servers']]
        trainer_endpoints = [j for i in peer_list for j in i['trainers']]
        #rank_offset = sum([i['replicas'] for i in peer_list[:rank]])

        server_rank_offset = sum([len(i['servers']) for i in peer_list[:rank]])
        trainer_rank_offset = sum(
            [len(i['trainers']) for i in peer_list[:rank]])

        self.pod.rank = rank

        self.pod.replicas = server_num + trainer_num

        for i in range(server_num):
            e = {
                f"{ENV_PREFIX}PSERVER_ENDPOINTS": ",".join(server_endpoints),
                f"{ENV_PREFIX}TRAINER_ENDPOINTS": ",".join(trainer_endpoints),
                f"{ENV_PREFIX}ROLE": "PSERVER",
                f"{ENV_PREFIX}RANK": "{}".format(i + server_rank_offset),
                "POD_IP": self.ctx.node.ip,
            }
            e.update(_gloo_envs)
            log_tag = "ps.{}".format(i)
            self.add_container(envs=e, log_tag=log_tag)

        for i in range(trainer_num):
            e = {
                f"{ENV_PREFIX}PSERVERS_IP_PORT_LIST": self.ctx.args.servers,
                f"{ENV_PREFIX}TRAINER_ENDPOINTS": self.ctx.args.trainers,
                f"{ENV_PREFIX}PORT": servers[i].split(":")[1],
                f"{ENV_PREFIX}ROLE": "TRAINER",
                "POD_IP": self.ctx.node.ip,
            }
            e.update(_gloo_envs)
            log_tag = "trainer.{}".format(i)
            self.add_container(envs=e, log_tag=log_tag)
