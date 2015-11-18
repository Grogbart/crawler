from queue_manager import QueueClient

__author__ = 'david'

alexa_splits = [1000000, 1000, 100]

# creates list from AlexaCSV with dated sources
def datedAlexaCSV(date, splits):
    splits.sort()
    m = splits[len(splits) - 1] + 1
    r = []
    f = open("top-1m.csv", "r")
    for i in range(1,m):
        c = str(i) + ','
        t = f.readline().replace('\n', '').replace(c, '')
        q = []
        for s in splits:
            q.append(date + "AlexaTOP" + str(s))
        r.append([t, q])
        i += 1
        if i > splits[0]:
            splits.remove(splits[0])
    f.close()
    return r


# downloads and unzips AlexaCSV and return the date
def getAlexaCSV():
    # TODO: downloading, unzipping and dating
    return "11_2015"



if __name__ == "__main__":

    # get instance of QueueClient
    c = QueueClient()

    # get appropriate queue from QueueClient
    host_queue = c.host_queue()

    d = getAlexaCSV()
    host_queue.put(datedAlexaCSV(d, alexa_splits))
