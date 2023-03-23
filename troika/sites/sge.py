"""SGE-managed site."""

import logging
import os
import pathlib
import re
import tempfile
import time

from .. import InvocationError, RunError
from ..connection import PIPE
from ..preprocess import preprocess
from ..utils import check_retcode
from .base import Site

_logger = logging.getLogger(__name__)


def _split_sge_directive(arg):
    """Split the argument of a SGE directive.

    >>> _split_sge_directive(b"-o foo")
    (b'-o', b'foo')
    >>> _split_sge_directive(b"-N job")
    (b'-N', b'job')
    >>> _split_sge_directive(b"-V")
    (b'-V', None)

    Args:
        arg(str) : arg

    Returns:
        tuple: key, value

    Raises:
        RunError: Malformed qsub argument

    """
    m = re.match(rb"(\S+)(\s+)?(.*)?$", arg)
    if m is None:
        raise RunError(r"Malformed qsub argument: {arg!r}")
    key, sep, val = m.groups()
    if sep is None:
        assert val == b""  # noqa S101
        val = None
    return key, val


_DIRECTIVE_RE = re.compile(rb"^#\s*\$\s+(.+)$")


@preprocess.register
def sge_add_output(sin, script, user, output):
    """Set the output file.

    Args:
        sin (_type_): _description_
        script (_type_): _description_
        user (_type_): _description_
        output (_type_): _description_

    Yields:
        _type_: _description_
    """
    for line in sin:
        m = _DIRECTIVE_RE.match(line)
        if m is None:
            yield line
            continue
        key, val = _split_sge_directive(m.group(1))
        if key in [b"-o", b"-e", b"-j"]:
            continue
        yield line
    yield b"#$ -e " + os.fsencode(output) + b"\n"
    yield b"#$ -o " + os.fsencode(output) + b"\n"


@preprocess.register
def sge_bubble(sin, script, user, output):
    """Make sure all SGE directives are at the top.

    Args:
        sin (_type_): _description_
        script (_type_): _description_
        user (_type_): _description_
        output (_type_): _description_

    Yields:
        _type_: _description_
    """
    directives = []
    with tempfile.SpooledTemporaryFile(
        max_size=1024**3, mode="w+b", dir=script.parent, prefix=script.name
    ) as tmp:
        first = True
        for line in sin:
            if line.isspace():
                tmp.write(line)
                continue

            m = _DIRECTIVE_RE.match(line)
            if m is not None:
                directives.append(line)
                continue

            if first:
                first = False
                if line.startswith(b"#!"):
                    yield line
                    continue

            tmp.write(line)

        yield from directives
        tmp.seek(0)
        yield from tmp


class SGESite(Site):
    """Site managed using SGE."""

    def __init__(self, config, connection, global_config):
        """Construct SGE.

        Args:
            config (_type_): _description_
            connection (_type_): _description_
            global_config (_type_): _description_
        """
        super().__init__(config, connection, global_config)
        self._qsub = config.get("qsub_command", "qsub")
        self._qdel = config.get("qdel_command", "qdel")
        self._qsig = config.get("qsig_command", "qsig")
        self._qstat = config.get("qstat_command", "qstat")
        self._copy_script = config.get("copy_script", False)

    def submit(self, script, user, output, dryrun=False):
        """See `troika.sites.Site.submit`.

        Args:
            script (_type_): _description_
            user (_type_): _description_
            output (_type_): _description_
            dryrun (bool, optional): _description_. Defaults to False.

        Raises:
            InvocationError: _description_

        Returns:
            _type_: _description_
        """
        script = pathlib.Path(script)
        sub_output = script.with_suffix(script.suffix + ".sub")
        if sub_output.exists():
            _logger.warning(
                "Submission output file %r already exists, overwriting",
                str(sub_output),
            )
        sub_error = script.with_suffix(script.suffix + ".suberr")
        if sub_error.exists():
            _logger.warning(
                "Submission error file %r already exists, overwriting",
                str(sub_error),
            )

        cmd = [self._qsub]

        if not script.exists():
            raise InvocationError(f"Script file {str(script)!r} does not exist")
        inpf = None
        if self._copy_script:
            script_remote = pathlib.PurePath(output).parent / script.name
            self._connection.sendfile(script, script_remote, dryrun=dryrun)
            cmd.append(script_remote)
        else:
            sge_job_file_path = script.with_suffix(script.suffix + ".sge")
            sge_job_file = pathlib.Path(sge_job_file_path)

            sge_lines = ""
            with open(script.resolve(), mode="r", encoding="utf-8") as fhandler:
                for line in fhandler.readlines():
                    if line.find("#$ ") == 0:
                        sge_lines = line + "" + sge_lines

            with open(sge_job_file.resolve(), mode="w", encoding="utf-8") as fhandler:
                fhandler.write("#!/bin/bash\n")
                fhandler.write(sge_lines)
                fhandler.write("\n")
                fhandler.write(f"{script.resolve()} || exit 1\n")
            inpf = sge_job_file.open(mode="rb")

        outf = None
        errf = None
        if not dryrun:
            outf = sub_output.open(mode="wb")
            errf = sub_error.open(mode="wb")

        proc = self._connection.execute(
            cmd, stdin=inpf, stdout=outf, stderr=errf, dryrun=dryrun
        )
        if dryrun:
            return

        retcode = proc.wait()
        check_retcode(
            retcode,
            what="Submission",
            suffix=f", check {str(sub_output)!r} and {str(sub_error)!r}",
        )

        jobid = sub_output.read_text().strip()
        _logger.debug("SGE job ID: %s", jobid)

        jid_output = script.with_suffix(script.suffix + ".jid")
        if jid_output.exists():
            _logger.warning(
                "Job ID output file %r already exists, overwriting", str(jid_output)
            )
        jid_output.write_text(str(jobid) + "\n")

        return jobid

    def monitor(self, script, user, jid=None, dryrun=False):
        """See `troika.sites.Site.monitor`.

        Args:
            script (_type_): _description_
            user (_type_): _description_
            jid (_type_, optional): _description_. Defaults to None.
            dryrun (bool, optional): _description_. Defaults to False.
        """
        script = pathlib.Path(script)

        if jid is None:
            jid = self._parse_jidfile(script)

        stat_output = script.with_suffix(script.suffix + ".stat")
        if stat_output.exists():
            _logger.warning(
                "Status file %r already exists, overwriting", str(stat_output)
            )
        outf = None
        if not dryrun:
            outf = stat_output.open(mode="wb")

        self._connection.execute([self._qstat, jid], stdout=outf, dryrun=dryrun)

        _logger.info("Output written to %r", str(stat_output))

    def kill(self, script, user, jid=None, dryrun=False):
        """See `troika.sites.Site.kill`.

        Args:
            script (_type_): _description_
            user (_type_): _description_
            jid (_type_, optional): _description_. Defaults to None.
            dryrun (bool, optional): _description_. Defaults to False.
        """
        script = pathlib.Path(script)

        if jid is None:
            jid = self._parse_jidfile(script)

        seq = self._kill_sequence
        if seq is None:
            seq = [(0, None)]

        first = True
        for wait, sig in seq:
            time.sleep(wait)

            cmd = [self._qdel, jid]
            if sig is not None:
                cmd = [self._qsig, "-s", str(int(sig)), jid]
            proc = self._connection.execute(cmd, stdout=PIPE, dryrun=dryrun)

            if dryrun:
                continue

            proc_stdout, _ = proc.communicate()
            retcode = proc.returncode
            if retcode != 0:
                if first:
                    _logger.error("qdel/qsig output: %s", proc_stdout)
                    check_retcode(retcode, what="Kill")
                else:
                    return

            first = False

    def _parse_jidfile(self, script):
        script = pathlib.Path(script)
        jid_output = script.with_suffix(script.suffix + ".jid")
        try:
            return jid_output.read_text().strip()
        except IOError as e:
            raise RunError(f"Could not read the job id: {e!s}")

    def __repr__(self):
        return f"{self.__class__.__name__}(connection={self._connection!r}, qsub_command={self._qsub!r})"
