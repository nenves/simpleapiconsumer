## smapiconsumer package

A simple package for consuming test social media api. 

### Installation
1. Clone repository [https://github.com/nenves/simpleapiconsumer.git](https://github.com/nenves/simpleapiconsumer.git)
2. Navigate to root folder. 
3. Create and activate a clean virtual environment
4. Install package smapiconsumer with `pip install .\dist\smapiconsumer-0.0.1-py3-none-any.whl` in that environment

### Usage
1. Create a copy of `configuration_template.json` as `configuration.json` in a working directory.
```
{
    "authurl" : "<Token registration URL here>",
    "url" : "<Post url here>",
    "client_id" : "<Client Id here>",
    "email" : "your email here",
    "name" : "<your name here>",
    "min_page_id" : 1,
    "max_page_id" : 10,
    "output_file" : "rawdata_{page_id}.json"
}
```
2. Edit the parameters in `configuration.json`  
3. WIth your virtual environment activated - from the command line run `python.exe -m smapiconsumer.consume_sm_api` 
4. Alternatively you can run `consume_sm_api.py` script from `src/smapiconsumer/` directory 
5. `statistics.json` file will be created in `output` folder under the working directory.
6. Raw data will be downloaded in `rawdata` folder under the working directory
