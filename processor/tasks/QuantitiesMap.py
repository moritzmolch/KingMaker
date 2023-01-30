import law
import luigi
import os
import json
from CROWNRun import CROWNRun
from framework import console
from framework import Task

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)



class QuantitiesMap(Task):
    scopes = luigi.ListParameter()
    all_sampletypes = luigi.ListParameter()
    all_eras = luigi.ListParameter()
    era = luigi.Parameter()
    sampletype = luigi.Parameter()
    production_tag = luigi.Parameter()
    analysis = luigi.Parameter()
    config = luigi.Parameter()
    nick = luigi.Parameter()


    def requires(self):
        return {"ntuples": CROWNRun(
                nick=self.nick,
                analysis=self.analysis,
                config=self.config,
                production_tag=self.production_tag,
                all_eras=self.all_eras,
                all_sampletypes=self.all_sampletypes,
                era=self.era,
                sampletype=self.sampletype,
            )}


    def output(self):
        target = self.remote_target("quantities_map.json")
        target.parent.touch()
        return target

    def run(self):
        output = self.output()
        _workdir = os.path.abspath("workdir")
        ensure_dir(_workdir)
        quantities_map = {}
        # go through all input files and get all quantities maps
        inputs = self.input()["ntuples"]
        print(inputs)
        for sample in inputs:
            # flatten the list of lists from self.input()['ntuples'][sample].iter_existing()
            inputfiles = [item for sublist in self.input()['ntuples'][sample].iter_existing() for item in sublist]
            for inputfile in inputfiles:
                if inputfile.path.endswith("quantities_map.json"):
                    console.log("Found quantities map in {}".format(inputfile.path))
                    with inputfile.localize("r") as _file:
                        # open file and update quantities map
                        quantities_map.update(json.load(open(_file.path, "r")))
        # write the quantities map to a file
        local_filename = os.path.join(_workdir, "quantities_map.json")
        with open(local_filename, "w") as f:
            json.dump(quantities_map, f, indent=4)
        output.copy_from_local(local_filename)