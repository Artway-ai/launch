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

from .context import Context
from . import controllers


def launch():
    # initialize the context to run
    ctx = Context()

    if ctx.is_legacy_mode():

        # legacy mode
        raise Exception('No legacy mode defined')

    else:

        # initialize the selected controller
        c = controllers.init(ctx)

        # run the pods
        c.run()

        # manager or just wait pod
        c.finalize()


if __name__ == "__main__":
    launch()
