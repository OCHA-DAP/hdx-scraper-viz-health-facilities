import argparse
import logging
import warnings
from geopandas import read_file
from os import getenv
from os.path import expanduser, join
from pandas import concat
from shapely.errors import ShapelyDeprecationWarning

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import temp_dir

from health_facilities import HealthFacilities

setup_logging()
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

lookup = "hdx-scraper-viz-health-facilities"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-hk", "--hdx_key", default=None, help="HDX api key")
    parser.add_argument("-ua", "--user_agent", default=None, help="user agent")
    parser.add_argument("-pp", "--preprefix", default=None, help="preprefix")
    parser.add_argument("-hs", "--hdx_site", default=None, help="HDX site to use")
    parser.add_argument("-co", "--countries", default=None, help="Which countries to update on HDX")
    args = parser.parse_args()
    return args


def main(
    countries,
    **ignore,
):
    logger.info(f"##### hdx-scraper-viz-health-facilities ####")
    configuration = Configuration.read()
    with temp_dir(folder="TempVizHealthFacilities") as temp_folder:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:

            # download subnational boundaries
            logger.info("Downloading subnational boundaries")
            subnational_json = dict()
            dataset = Dataset.read_from_hdx(configuration["inputs"]["boundaries"])
            for resource in dataset.get_resources():
                if "polbnda_adm" not in resource["name"]:
                    continue
                level = resource["name"][11]
                _, resource_file = resource.download(folder=temp_folder)
                subnational_json[level] = read_file(resource_file)

            # merge boundaries, adding required fields
            for level in subnational_json:
                add_levels = [i for i in range(1, 4) if not i == level]
                for add_level in add_levels:
                    subnational_json[level][f"ADM{add_level}_PCODE"] = None
                    subnational_json[level][f"ADM{add_level}_REF"] = None
                subnational_json[level]["ADM_LEVEL"] = int(level)
                subnational_json[level]["ADM_PCODE"] = subnational_json[level][f"ADM{level}_PCODE"]
                subnational_json[level]["ADM_REF"] = subnational_json[level][f"ADM{level}_REF"]
            subnational_json = subnational_json.values()
            subnational_json = concat(subnational_json)
            subnational_json["Health_Facilities"] = None

            if not countries:
                countries = list(set(subnational_json["alpha_3"]))
            countries.sort()

            health_fac = HealthFacilities(
                configuration,
                downloader,
                subnational_json,
                temp_folder,
            )
            updated_countries = health_fac.run(countries)
            if len(updated_countries) > 0:
                updated_data, resource = health_fac.update_hdx_resource(configuration["inputs"]["dataset"],
                                                                        updated_countries)

                # update hdx
                updated_data.to_csv(join(temp_folder, "subnational_health_facilities.csv"), index=False)
                resource.set_file_to_upload(join(temp_folder, "subnational_health_facilities.csv"))
                try:
                    resource.update_in_hdx()
                except HDXError:
                    logger.exception("Could not update resource")


if __name__ == "__main__":
    args = parse_args()
    hdx_key = args.hdx_key
    if hdx_key is None:
        hdx_key = getenv("HDX_KEY")
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv("USER_AGENT")
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = getenv("PREPREFIX")
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = getenv("HDX_SITE", "prod")
    countries = args.countries
    if countries is None:
        countries = getenv("COUNTRIES", None)
    if countries:
        countries = countries.split(",")
    facade(
        main,
        hdx_key=hdx_key,
        user_agent=user_agent,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        preprefix=preprefix,
        hdx_site=hdx_site,
        project_config_yaml=join("config", "project_configuration.yml"),
        countries=countries,
    )
