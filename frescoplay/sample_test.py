from email import header
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum
import os


class sampleTest(unittest.TestCase):
    sample_score = {"latest_empolyees": 0,
                    "Gender_Wise": 0
                    }

    @classmethod
    def setUpClass(cls):
        """
        Start Spark, define config and path to test
        """

        cls.spark = SparkSession \
            .builder \
            .appName("sampleTest") \
            .master("local") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    def test_result_1(self):
        self.sample("latest_empolyees", 1000)

    def test_result_2(self):
        self.sample("Gender_Wise", 3)

    def sample(self, file_name, expected_count):
        input_df = self.read_file(file_name)
        expected_headers = {
            "latest_empolyees": ["Variable", "Sex", "Alberta", "British_Columbia", "Manitoba", "Brunswick",
                                 "Newfoundland_and_Labrador", "Nova_Scotia", "Ontario", "Prince_Edward_Island",
                                 "Quebec", "Saskatchewan", "Year", "Month_Name"],
            "Gender_Wise": ["Sex", "Sum_Alberta", "Sum_British_Columbia", "Sum_Manitoba", "Sum_Brunswick",
                            "Sum_Newfoundland_and_Labrador", "Sum_Nova_Scotia", "Sum_Ontario",
                            "Sum_Prince_Edward_Island", "Sum_Quebec", "Sum_Saskatchewan"]
        }
        if input_df != None:
            header_check = 0
            actual_count = input_df.count()
            cnt = 0
            if input_df.columns == expected_headers[file_name]:
                header_check += 10
            else:
                print("header does not match for the file: %s", file_name)

            self.assertEqual(actual_count, expected_count, "Count of records does not match")
            if actual_count == expected_count:
                cnt += 90
            tot = header_check + cnt
            self.sample_score[file_name] = tot
        else:
            self.fail("The required input file seems missing")

    def read_file(self, file_name):
        try:
            cwd = os.getcwd()
            path = cwd + "/output/" + file_name
            print(path)
            df = self.spark.read.csv(path, header=True)
            return df

        except:
            print(
                "----------------------------------------------------------------------------------------------------------")
            print("Looks like the output directory for following file is missing.{}".format(file_name))
            print(
                "Please check whether the output file directory name matches the directory name given in instructions.")
            print("Note that you can still go ahead and submit your test. Scoring will happen accordingly.")
            print(
                "----------------------------------------------------------------------------------------------------------")

    @classmethod
    def tearDownClass(cls):
        """
        Stop Spark
        """
        print("          ")
        print(cls.sample_score)
        print("         ")
        test_score = 0
        for i, j in cls.sample_score.items():
            test_score = test_score + j
        test_score = test_score / 2
        print("**********************************************************")
        print("Sample_Score:", test_score)
        print("               ")
        print("NOTE: The sample score does not represent the Final Score.")
        print("**********************************************************")
        cls.spark.stop()


if __name__ == "__main__":
    unittest.main()



