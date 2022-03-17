import unittest
from smapiconsumer import consume_sm_api as sut 
import multiprocessing
import asyncio
import json
import pandas as pd
from datetime import datetime

def harness_request_new_token():
    config = sut.read_configuration()
    with multiprocessing.Manager() as manager:
        apikey = manager.dict()
        apikey["token"] = "123"
        apikey["isvalid"] = datetime.now()
        asyncio.run(sut.request_new_token(config,apikey))
        token = apikey["token"]
    return token


class TestConsumeSMAPI(unittest.TestCase):

    @unittest.skip("already tested")
    def test_request_new_token(self):
        token = harness_request_new_token()
        self.assertNotEqual(token, "123")

    @unittest.skip("already tested")
    def test_analyze_posts_in_response(self):
        with open("response.json") as f:
            resp = json.load(f)

        max_df, mean_df, tot_month_df, tot_user_month_df = sut.analyze_posts_in_response(resp)
        self.assertEqual(max_df.shape[0],2)
        self.assertEqual(mean_df.shape[0],2)
        self.assertEqual(tot_month_df.shape[0],4)
        self.assertEqual(tot_user_month_df.shape[0],33)

    @unittest.skip("already tested")
    def test_formating_out_file_string(self):
        config = sut.read_configuration()
        s = config["output_file"].format(page_id=10)
        self.assertEqual(s, "rawdata_10.json")

    @unittest.skip("already tested")
    def test_generate_statistics(self):
        with open("max.json") as maxf, open("mean.json") as meanf, open("tot_mon.json") as totmonf, open("tot_usr_mon.json") as totmonusrf:
            max_df=pd.DataFrame(json.load(maxf))
            mean_df=pd.DataFrame(json.load(meanf))
            tot_mon_df=pd.DataFrame(json.load(totmonf))
            tot_mon_usr_df = pd.DataFrame(json.load(totmonusrf))
        
            a,b,c,d = sut.generate_statistics(max_df, mean_df, tot_mon_df, pd.concat([tot_mon_usr_df, tot_mon_usr_df]))

            res_dict = {
                "maximum length per month": json.loads(a.to_json(orient="records")),
                "average length per month": json.loads(b.to_json(orient="records")),
                "total posts by week number": json.loads(c.to_json(orient="records")),
                "average posts per user per month": json.loads(d.to_json(orient="records"))
            }

            with open('data.json', 'w') as fp:
                json.dump(res_dict, fp, indent=2)

    @unittest.skip("already tested")
    def test_process_page(self):
        config = sut.read_configuration()
        connparams = {
            "url": config["url"],
            "authurl": config["authurl"],
            "client_id": config["client_id"],
            "email": config["email"],
            "name": config["name"]
        }
        with multiprocessing.Manager() as manager:
            lock = manager.Lock()
            apikey = manager.dict()
            apikey["token"] = "123"
            apikey["isvalid"] = datetime.now()
            res = asyncio.run(sut.process_page(lock, apikey, connparams, 2, config["output_file"]))
            print(res)

    @unittest.skip("already tested")
    def test_process_page_group(self):
        config = sut.read_configuration()
        connparams = {
            "url": config["url"],
            "authurl": config["authurl"],
            "client_id": config["client_id"],
            "email": config["email"],
            "name": config["name"]
        }
        with multiprocessing.Manager() as manager:
            lock = manager.Lock()
            apikey = manager.dict()
            apikey["token"] = "123"
            apikey["isvalid"] = datetime.now()
            res = asyncio.run(sut.process_page_group(lock, apikey, connparams, 1, 2, config["output_file"]))
            print(res)
    
    @unittest.skip("already tested")
    def test_reding_config_file(self):
        conf = sut.read_configuration()
        print(conf)

if __name__ == '__main__':
    unittest.main()
