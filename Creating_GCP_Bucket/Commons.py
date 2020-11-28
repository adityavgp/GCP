import json

class Commons:
    def __init__(self, jsonfile):
        self.jsonfile =jsonfile

    def getProjectId(self, jsonfile):
        with open(jsonfile, ) as jsonob:
            getdata = json.load(jsonob)
            print(getdata['project_id'])
        return getdata['project_id']