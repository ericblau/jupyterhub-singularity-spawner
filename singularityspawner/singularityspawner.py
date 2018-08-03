# Copyright (c) 2017, Zebula Sampedro, CU Boulder Research Computing

"""
Singularity Sudo Spawner

SingularitySpawner provides a mechanism for spawning Jupyter Notebooks inside of Singularity containers. The spawner options form is leveraged such that the user can specify which Singularity image the spawner should use.

A `singularity exec {notebook spawn cmd}` is used to start the notebook inside of the container.
"""
import os
import pipes
import json
import shutil
import sys


from tornado import gen
from tornado.ioloop import IOLoop
from tornado.process import Subprocess
from tornado.iostream import StreamClosedError
from singularity.cli import Singularity

from traitlets import List, Unicode, Bool

from jupyterhub.spawner import (
    LocalProcessSpawner, set_user_setuid
)
from jupyterhub.utils import random_port
from jupyterhub.traitlets import Command
from traitlets import (
    Integer, Unicode, Float, Dict, List, Bool, default
)

JS_SCRIPT = """<script>
require(['jquery'], function($) {
  var pullCheckbox = $('#pull-checkbox');
  var spawnButton = $('#spawn_form input[type="submit"]');

  pullCheckbox.on('change', function() {
    $('#url-group').toggle();
  });

  spawnButton.on('click', function() {
    if (pullCheckbox.is(':checked')) {
      $(this).attr("value","Pulling image...")
    }
  })
});
</script>
"""

class SingularitySpawner(LocalProcessSpawner):
    """SingularitySpawner - extends the default LocalProcessSpawner to allow for:
    1) User-specification of a singularity image via the Spawner options form
    2) Spawning a Notebook server within a Singularity container
    """

    singularity_cmd = Command(['/usr/bin/singularity','exec'],
        help="""
        This is the singularity command that will be executed when starting the
        single-user server. The image path and notebook server args will be concatenated to the end of this command. This is a good place to
        specify any site-specific options that should be applied to all users,
        such as default mounts.
        """
    ).tag(config=True)

    notebook_cmd = Command(['/opt/conda/bin/jupyterhub-singleuser'],
        help="""
        The command used for starting the single-user server.
        Provide either a string or a list containing the path to the startup script command. Extra arguments,
        other than this path, should be provided via `args`.
        """
    ).tag(config=True)

    default_image_path = Unicode('',
        help="""
        Absolute POSIX filepath to Singularity image that will be used to
        execute the notebook server spawn command, if another path is not
        specified by the user.
        """
    ).tag(config=True)

    pull_from_url = Bool(False,
        help="""
        If set to True, the user should be presented with URI specification
        options, and the spawner should first pull a new image from the
        specified shub or docker URI prior to running the notebook command.
        In this configuration, the `user_image_path` will specify where the
        new container will be created.
        """
    ).tag(config=True)

    default_image_url = Unicode('docker://jupyter/base-notebook',
        help="""
        Singularity Hub or Docker URI from which the notebook image will be
        pulled, if no other URI is specified by the user but the _pull_ option
        has been selected.
        """
    ).tag(config=True)

    options_form = Unicode()

    form_template = Unicode(
        """

<!-- Multiple Radios -->
<div class="form-group">
  <label class="col-md-4 control-label" for="images">Select an Image</label>
  <div class="col-md-4">
  <div class="radio">
    <label for="images-0">
      <input type="radio" name="user_image_path" id="images-0" value="/mnt/images/ub-jup.img" checked="checked">
      Ubuntu Jupyterlab
    </label>
	</div>
  <div class="radio">
    <label for="images-1">
      <input type="radio" name="user_image_path" id="images-1" value="/mnt/images/ubuntu-scipy.img">
	ubuntu-scipy
    </label>
	</div>
  </div>
</div>
        """
    )
    singularitysudospawner_path = Unicode(shutil.which('singularitysudospawner') or 'singularitysudospawner', config=True,
        help="Path to singularitysudospawner script"
    )
    sudo_args = List(['-nH'], config=True,
        help="Extra args to pass to sudo"
    )
    debug_mediator = Bool(True, config=True,
        help="Extra log output from the mediator process for debugging",
    )

    def format_default_image_path(self):
        """Format the image path template string."""
        format_options = dict(username=self.user.escaped_name)
        default_image_path = self.default_image_path.format(**format_options)
        return default_image_path

    @default('options_form')
    def _options_form(self):
        """Render the options form."""
        default_image_path = self.format_default_image_path()
        format_options = dict(default_image_path=default_image_path,default_image_url=self.default_image_url)
        options_form = self.form_template.format(**format_options)
        return JS_SCRIPT + options_form

    def options_from_form(self, form_data):
        """Get data from options form input fields."""
        user_image_path = form_data.get('user_image_path', None)
        user_image_url = form_data.get('user_image_url', None)
        pull_from_url = form_data.get('pull_from_url',False)

        return dict(user_image_path=user_image_path,user_image_url=user_image_url,pull_from_url=pull_from_url)

    def get_image_url(self):
        """Get image URL to pull image from user options or default."""
        default_image_url = self.default_image_url
        image_url = self.user_options.get('user_image_url',[default_image_url])
        return image_url

    def get_image_path(self):
        """Get image filepath specified in user options else default."""
        default_image_path = self.format_default_image_path()
        image_path = self.user_options.get('user_image_path',[default_image_path])
        return image_path

    @gen.coroutine
    def pull_image(self,image_url):
        """Pull the singularity image to specified image path."""
        image_path = self.get_image_path()
        s = Singularity()
        container_path = s.pull(image_url[0],image_name=image_path[0])
        return Unicode(container_path)

    def _build_cmd(self):
        image_path = self.get_image_path()
        cmd = []
        cmd.extend(self.singularity_cmd)
        cmd.extend(image_path)
        cmd.extend(self.notebook_cmd)
        return cmd

    @property
    def cmd(self):
        return self._build_cmd()

    @gen.coroutine
    def relog_stderr(self, stderr):
        while not stderr.closed():
            try:
                line = yield stderr.read_until(b'\n')
            except StreamClosedError:
                return
            else:
                # TODO: log instead of write to stderr directly?
                # If we do that, will get huge double-prefix messages:
                # [I date JupyterHub] [W date SingleUser] msg...
                sys.stderr.write(line.decode('utf8', 'replace'))

    def make_preexec_fn(self):
        return None

    @gen.coroutine
    def do(self, action, **kwargs):
        """Instruct the mediator process to take a given action"""
        kwargs['action'] = action
        kwargs['singularitycmd'] = self.cmd
        cmd = ['sudo', '-u', self.user.escaped_name]
        cmd.extend(self.sudo_args)
        cmd.append(self.singularitysudospawner_path)
        cmd.append('--logging=debug')
        if self.debug_mediator:
            cmd.append('--logging=debug')

        self.log.debug("Spawning %s %s", cmd, kwargs['singularitycmd'])
        p = Subprocess(cmd, stdin=Subprocess.STREAM, stdout=Subprocess.STREAM, stderr=Subprocess.STREAM, preexec_fn=self.make_preexec_fn())
        stderr_future = self.relog_stderr(p.stderr)
        # hand the stderr future to the IOLoop so it isn't orphaned,
        # even though we aren't going to wait for it unless there's an error
        IOLoop.current().add_callback(lambda : stderr_future)

        yield p.stdin.write(json.dumps(kwargs).encode('utf8'))
        p.stdin.close()
        data = yield p.stdout.read_until_close()
        if p.returncode:
            yield stderr_future
            raise RuntimeError("sudospawner subprocess failed with exit code: %r" % p.returncode)

        data_str = data.decode('utf8', 'replace')

        try:
            data_str = data_str[data_str.index('{'):data_str.rindex('}')+1]
            response = json.loads(data_str)
            f = open("/tmp/mediatorresponse.txt", "a")
            f.write(data_str)
        except ValueError:
            self.log.error("Failed to get JSON result from mediator: %r" % data_str)
            raise
        return response

    @gen.coroutine
    def _signal(self, sig):
        if sig == 0:
            # short-circuit existence check without invoking sudo
            try:
                os.kill(self.pid, sig)
            except ProcessLookupError:
                # No such process
                return False
            except PermissionError:
                # When running hub with reduced permissions,
                # we won't have permission to send signals, even 0.
                # PermissionError means process exists.
                pass
            return True
        reply = yield self.do('kill', pid=self.pid, signal=sig)
        return reply['alive']

    @gen.coroutine
    def start(self):
        self.port = random_port()

        # only args, not the base command
        reply = yield self.do(action='spawn', args=self.get_args(), env=self.get_env())
        self.pid = reply['pid']
        print(self.ip)
        # 0.7 expects ip, port to be returned
        return (self.ip or '127.0.0.1', self.port)

    @gen.coroutine
    def foostart(self):
        """
        Start the single-user server in the Singularity container specified
        by image path, pulling from docker or shub first if the pull option
        is selected.
        """
        image_path = self.get_image_path()
        pull_from_url = self.user_options.get('pull_from_url',False)
        if pull_from_url:
            image_url = self.get_image_url()
            self.pull_image(image_url)

        super(SingularitySpawner,self).start()
