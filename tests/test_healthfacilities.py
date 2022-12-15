from geopandas import read_file
from os.path import join
from pandas import read_csv
from pandas.testing import assert_frame_equal

import pytest
from hdx.api.configuration import Configuration
from hdx.utilities.path import temp_dir
from hdx.utilities.useragent import UserAgent
from health_facilities import HealthFacilities


class TestHealthFacilities:
    countries = ["COL"]
    subnational_json = read_file(join("tests", "fixtures", "subnational_json.geojson"))
    summarized_data = read_csv(join("tests", "fixtures", "summarized_data.csv"))
    hotosm_data = read_file(
        join("tests",
             "fixtures",
             "hotosm_col_health_facilities_points_shp",
             "hotosm_col_health_facilities_points.shp")
    )

    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def find_read_resource(self):
        return self.hotosm_data

    def test_summarize_data(self, configuration):
        with temp_dir("TestVizHealthFacilities", delete_on_success=True, delete_on_failure=False) as temp_folder:
            health_fac = HealthFacilities(configuration, self.subnational_json, temp_folder)
            summarized_data, updated_countries = health_fac.summarize_data(self.countries)
            assert updated_countries == {1: ["COL"]}
            assert_frame_equal(summarized_data, self.summarized_data)
