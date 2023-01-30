import pdb
import law
import luigi
import os
from CROWNBuildFriend import CROWNBuildFriend
from CROWNRun import CROWNRun
import tarfile
from ConfigureDatasets import ConfigureDatasets
import subprocess
import time
import copy
from framework import console
from framework import HTCondorWorkflow

law.contrib.load("wlcg")


def create_abspath(file_path):
    if not os.path.exists(file_path):
        os.makedirs(file_path)


class CROWNFriends(HTCondorWorkflow):
    """
    Gather and compile CROWN with the given configuration
    """

    output_collection_cls = law.NestedSiblingFileCollection

    # scopes = luigi.ListParameter()
    all_sampletypes = luigi.ListParameter()
    all_eras = luigi.ListParameter()
    scopes = luigi.ListParameter()
    analysis = luigi.Parameter()
    friend_config = luigi.Parameter()
    config = luigi.Parameter()
    friend_name = luigi.Parameter()
    nick = luigi.Parameter()
    sampletype = luigi.Parameter()
    era = luigi.Parameter()
    production_tag = luigi.Parameter()
    files_per_task = luigi.IntParameter()
    branch_map = {}

    def htcondor_job_config(self, config, job_num, branches):
        config = super().htcondor_job_config(config, job_num, branches)
        config.custom_content.append(
            (
                "JobBatchName",
                f"{self.nick}-{self.analysis}-{self.friend_config}-{self.production_tag}",
            )
        )
        return config

    def modify_polling_status_line(self, status_line):
        """
        Hook to modify the status line that is printed during polling.
        """
        name = f"{self.nick} (Analysis: {self.analysis} Config: {self.friend_config} Tag: {self.production_tag})"
        return f"{status_line} - {law.util.colored(name, color='light_cyan')}"

    def workflow_requires(self):
        requirements = {}
        # requirements["dataset"] = {}
        # for i, nick in enumerate(self.details):
        #     requirements["dataset"][i] = ConfigureDatasets.req(
        #         self, nick=nick, production_tag=self.production_tag
        #     )
        requirements["ntuples"] = CROWNRun.req(self)
        requirements["friend_tarball"] = CROWNBuildFriend.req(self)

        return requirements

    def requires(self):
        return {"friend_tarball": CROWNBuildFriend.req(self)}

    def create_branch_map(self):
        print("Creating branch map")
        start = time.time()
        branch_map = {}
        counter = 0
        inputs = self.input()["ntuples"]["collection"]
        # get all files from the dataset, including missing ones
        branches = [item for subset in inputs.iter_existing() for item in subset]
        branches += [item for subset in inputs.iter_missing() for item in subset]
        # print(branches)
        for inputfile in branches:
            if not inputfile.path.endswith(".root"):
                continue
            # identif the scope from the inputfile
            scope = inputfile.path.split("/")[-2]
            if scope in self.scopes:
                branch_map[counter] = {
                    "scope": scope,
                    "nick": self.nick,
                    "era": self.era,
                    "sampletype": self.sampletype,
                    "inputfile": os.path.expandvars(self.wlcg_path) + inputfile.path,
                }
                counter += 1
        print("Time to create branch map: {}".format(time.time() - start))
        # print(branch_map)
        return branch_map

    def output(self):
        targets = []
        print("Creating output targets")
        start = time.time()
        nicks = [
            "{friendname}/{era}/{nick}/{scope}/{nick}_{branch}.root".format(
                friendname=self.friend_name,
                era=self.branch_data["era"],
                nick=self.branch_data["nick"],
                branch=self.branch,
                scope=scope,
        ) for scope in self.scopes]
        print("Time to create output targets: {}".format(time.time() - start))
        targets = self.remote_targets(nicks)
        # print(targets)
        for target in targets:
            target.parent.touch()
        return targets

    def run(self):
        outputs = self.output()
        branch_data = self.branch_data
        _base_workdir = os.path.abspath("workdir")
        create_abspath(_base_workdir)
        _workdir = os.path.join(
            _base_workdir, f"{self.production_tag}_{self.friend_name}"
        )
        create_abspath(_workdir)
        _inputfile = branch_data["inputfile"]
        # set the outputfilename to the first name in the output list, removing the scope suffix
        _outputfile = str(
            outputs[0].basename.replace("_{}.root".format(self.scopes[0]), ".root")
        )
        _abs_executable = "{}/{}_{}_{}".format(
            _workdir, self.friend_config, branch_data["sampletype"], branch_data["era"]
        )
        console.log(
            "Getting CROWN tarball from {}".format(self.input()["tarball"].uri())
        )
        with self.input()["friend_tarball"].localize("r") as _file:
            _tarballpath = _file.path
        # first unpack the tarball if the exec is not there yet
        tempfile = os.path.join(
                _workdir,
                "unpacking_{}_{}_{}".format(
                    self.config, branch_data["sampletype"], branch_data["era"]
                ),
            )
        while os.path.exists(tempfile):
            time.sleep(1)
        if not os.path.exists(_abs_executable):
            # create a temp file to signal that we are unpacking
            open(tempfile,"a",).close()
            tar = tarfile.open(_tarballpath, "r:gz")
            tar.extractall(_workdir)
            os.remove(tempfile)
        # set environment using env script
        my_env = self.set_environment("{}/init.sh".format(_workdir))
        _crown_args = [_outputfile] + [_inputfile]
        _executable = "./{}_{}_{}".format(
            self.friend_config, branch_data["sampletype"], branch_data["era"]
        )
        # actual payload:
        console.rule("Starting CROWNFriends")
        console.log("Executable: {}".format(_executable))
        console.log("inputfile {}".format(_inputfile))
        console.log("outputfile {}".format(_outputfile))
        console.log("workdir {}".format(_workdir))  # run CROWN
        with subprocess.Popen(
            [_executable] + _crown_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            universal_newlines=True,
            env=my_env,
            cwd=_workdir,
        ) as p:
            for line in p.stdout:
                if line != "\n":
                    console.log(line.replace("\n", ""))
            for line in p.stderr:
                if line != "\n":
                    console.log("Error: {}".format(line.replace("\n", "")))
        if p.returncode != 0:
            console.log(
                "Error when running crown {}".format(
                    [_executable] + _crown_args,
                )
            )
            console.log("crown returned non-zero exit status {}".format(p.returncode))
            raise Exception("crown failed")
        else:
            console.log("Successful")
        console.log("Output files afterwards: {}".format(os.listdir(_workdir)))
        for i, outputfile in enumerate(outputs):
            outputfile.parent.touch()
            local_filename = os.path.join(
                _workdir,
                _outputfile.replace(".root", "_{}.root".format(self.scopes[i])),
            )
            # for each outputfile, add the scope suffix
            outputfile.copy_from_local(local_filename)
        console.rule("Finished CROWNFriends")
