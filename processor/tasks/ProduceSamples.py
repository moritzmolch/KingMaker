import luigi
import yaml
from CROWNRun import CROWNRun
from framework import console
from law.task.base import WrapperTask
from rich.table import Table

class ProduceSamples(WrapperTask):
    """
    collective task to trigger ntuple production for a list of samples
    """

    sample_list = luigi.Parameter()
    analysis = luigi.Parameter()
    config = luigi.Parameter()
    dataset_database = luigi.Parameter()
    production_tag = luigi.Parameter()
    scopes = luigi.ListParameter()

    def requires(self):
        # load the list of samples to be processed
        data = {}
        data["sampletypes"] = set()
        data["eras"] = set()
        data["details"] = {}
        samples = []
        # check if sample list is a file or a comma separated list
        if self.sample_list.endswith(".txt"):
            with open(self.sample_list) as file:
                samples = [nick.replace("\n", "") for nick in file.readlines()]
        elif "," in self.sample_list:
            samples = self.sample_list.split(",")
        else:
            samples = [self.sample_list]
        console.rule("")
        console.log(f"Production tag: {self.production_tag}")
        console.log(f"Analysis: {self.analysis}")
        console.log(f"Config: {self.config}")
        console.rule("")
        table = Table(title="Samples to be processed")

        table.add_column("Samplenick", justify="left")
        table.add_column("Era", justify="left")
        table.add_column("Sampletype", justify="left")

        for i, nick in enumerate(samples):
            data["details"][nick] = {}
            # check if sample exists in datasets.yaml
            with open(self.dataset_database, "r") as stream:
                sample_db = yaml.safe_load(stream)
            if nick not in sample_db:
                console.log(
                    "Sample {} not found in {}".format(nick, self.dataset_database)
                )
                raise Exception("Sample not found in DB")
            sample_data = sample_db[nick]
            data["details"][nick]["era"] = str(sample_data["era"])
            data["details"][nick]["sampletype"] = sample_data["sample_type"]
            # all samplestypes and eras are added to a list,
            # used to built the CROWN executable
            data["eras"].add(data["details"][nick]["era"])
            data["sampletypes"].add(data["details"][nick]["sampletype"])
            table.add_row(nick, data['details'][nick]['era'] ,data['details'][nick]['sampletype'])
        console.log(table)

        console.log(
            f"Producing ntuples for {len(data['details'])} samples in {len(data['eras'])} eras and {len(self.scopes)} scopes"
        )
        console.rule("")
        requirements = {}
        requirements["CROWNRun"] = CROWNRun(
            analysis=self.analysis,
            config=self.config,
            production_tag=self.production_tag,
            eras=data["eras"],
            sampletypes=data["sampletypes"],
            details=data["details"],
        )

        return requirements

    def run(self):
        pass
