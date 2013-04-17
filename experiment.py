import urllib, httplib, json, random
import subprocess, os, glob, time
import signal

class Server(object):
    def __init__(self):
        DEVNULL = open(os.devnull, 'wb')
        self.p = subprocess.Popen("./build/hyrise_server", stdout=DEVNULL)
        time.sleep(3)
        self.port = int(open("hyrise_server.port").readlines()[0])
        assert(self.port != 0)
        print "started server", self.port

    def query(self, d):
        try:
            #print json.dumps(d, sort_keys=True, indent=4)
            conn = httplib.HTTPConnection("localhost", self.port, strict=False)
            conn.request("POST", "", urllib.urlencode([("query",json.dumps(d))]))
            r = conn.getresponse()
            data = r.read()
            conn.close()
        except Exception, e:
            print e, d
        return data

    def __del__(self):
        self.p.send_signal(signal.SIGINT)
        self.p.wait()
        
def unique_filename():
    return str(time.time())

def assure_dir_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

def write_result(ppath, result):
    path = os.path.join("results", ppath)
    assure_dir_exists(path)
    fpath = os.path.join(path, unique_filename())
    with open(fpath, "w") as f:
        f.write(result)
    
def execute(query, query_group="default"):
    global server
    result = server.query(query)
    r = eval(result)
    if r.has_key("error"):
        print query
        print result
        exit()
    if r.has_key("rows"):
        print len(r["rows"])
    write_result(query_group, result)

def index_names(table, column):
    return table + str(column) + "_main", table + str(column) + "_delta"

def load(mainfile, deltafile, table, layout):
    load_plan = {
        "operators" : {
            "load": { "type" : "LoadMainDelta", 
                      "header": layout,
                      "mainfile": mainfile,
                      "deltafile": deltafile },
            "set": { "type" : "SetTable", 
                     "name" : table },
            "noop": { "type" : "NoOp" }
            },
        "edges": [["load", "set"], ["set", "noop"]]
        }
    execute(load_plan)

def create_index(table, column):
    main_idx, delta_idx = index_names(table, column)
    index_plan = {
        "operators" : {
            "get" : { "type": "GetTable",
                      "name": table },
            "exMain" : { "type" : "ExtractMain" },
            "exDelta" : {"type" : "ExtractDelta" },
            "idxMain" : {"type" : "CreateIndex",
                         "table_name" : main_idx,
                         "fields" : [column] },
            "idxDelta" : {"type" : "CreateIndex",
                          "table_name" : delta_idx,
                          "fields" : [column] },
            "noop": { "type" : "NoOp" }
            },
        "edges" : [["get", "exMain"], ["get", "exDelta"], ["exMain", "idxMain"], ["exDelta", "idxDelta"], ["exMain", "noop"], ["exDelta", "noop"]]
        }
    execute(index_plan)

def baseplan(table, papi):
    bp = { 
        #"limit" : 1, #only retrieve one result row
        "papi" : papi,
        "operators" : {
            "get" : {"type" : "GetTable",
                     "name" : table},
            "exMain" : { "type" : "ExtractMain" },
            "exDelta" : {"type" : "ExtractDelta" },
            "mergeMain" : { "type": "MergePositions" },
            "mergeDelta" : {"type": "MergePositions" },
            "materialize" : { "type" : "MaterializingMainDelta" },
            #"noop" : {"type": "NoOp"}
            },
        "edges" : [["get", "exMain"], 
                   ["get", "exDelta"], 
                   ["mergeMain", "materialize"], 
                   ["mergeDelta", "materialize"], 
                   #["materialize", "noop"]
                   ]
        }
    return bp

def index_selection(wing, index, column, value):
    return { "type": "IndexScan",
             "fields": [column],
             "index": index,
             "value": value,
             "vtype": 0 }

def index_range(wing, index, column, values):
    return { "type" : "IndexRangeScan",
             "fields": [column],
             "index" : index,
             "value_from": values[0],
             "value_to": values[1] }

def scan_selection(wing, index, column, value): 
    if (wing == "Main"):
        return { "type": "SimpleTableScan",
                 "materializing": False,
                 "predicates": [
                {"type": "EQ", "in": 0, "f": column, "value": value}
                ]}
    elif (wing == "Delta"):
        return { "type": "SimpleRawTableScan",
                 "materializing": False,
                 "predicates": [
                {"type": "EQ_R", "in": 0, "f": column, "value": value}
                ]}
    else:
        print "unmatched", wing

def scan_range(wing, index, column, values):
    if (wing == "Main"):
        return { 
            "type": "SimpleTableScan",
            "materializing": False,
            "predicates": [{"type": "BETWEEN", "in": 0, "f": column, "value_from": values[0], "value_to": values[1]}]
            }
    elif (wing == "Delta"):
        return { 
            "type": "SimpleRawTableScan",
            "materializing": False,
            "predicates": [{"type": "BETWEEN_R", "in": 0, "f": column, "value_from": values[0], "value_to": values[1]}]
            }
    else:
        print "unmatched", wing

def get_start_end(side):
    return "ex"+side, "merge"+side

def query(table, column_values, selection_operator_generator, papi, combo_str):
    bp = baseplan(table, papi)
    for index, item in enumerate(column_values.iteritems()):
        column, value = item
        for full, side in zip(["Main", "Delta"], index_names(table, column)):
            start_op, end_op = get_start_end(full)
            op_name = "%s_scan_%s" % (side, index)
            bp["operators"][op_name] = selection_operator_generator(full, side, column, value)
            bp["edges"] += [[start_op, op_name], [op_name, end_op]]
    return execute(bp, combo_str)

RATIOS = [0.5, 0.2, 0.1, 0.01, 0.001, 0.0001, 0.00001, 0.000001]
SCALES = [10, 20, 40]
def _setup(fname):
    for sf in SCALES:
        print "generating", sf
        di = "sf%s" % sf
        assure_dir_exists(di)
        subprocess.call(["./build/perf_datagen", "-w", str(sf), "-d", di, "--hyrise"])

    subprocess.call(["touch", ".setupdone"])

def setup(fname):
    if not os.path.exists(".setupdone"):
        _setup(fname)

def extract_ratio(name):
    return int(name[name.rindex("_")+1:])

SCAN_TYPES = { #"index_selection" : index_selection,
               "index_range" : index_range,
               #"scan_selection": scan_selection,
               "scan_range" : scan_range }

RUNS = 1

HEADERS = { 
    "column": """OL_O_ID|OL_D_ID|OL_W_ID|OL_NUMBER|OL_I_ID|OL_SUPPLY_W_ID|OL_DELIVERY_D|OL_QUANTITY|OL_AMOUNT|OL_DIST_INFO
INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|STRING|FLOAT|FLOAT|STRING
0_R|1_R|2_R|3_R|4_R|5_R|6_R|7_R|8_R|9_R""", }
   #"row": """OL_O_ID|OL_D_ID|OL_W_ID|OL_NUMBER|OL_I_ID|OL_SUPPLY_W_ID|OL_DELIVERY_D|OL_QUANTITY|OL_AMOUNT|OL_DIST_INFO
#INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|STRING|FLOAT|FLOAT|STRING
#0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R|0_R""" }

values = {
    0 : lambda: random.randint(0, 3000),
    3 : lambda x: random.randint(0, x)
}

server = None

def delete(f):
    subprocess.call(["rm", f])

def filenames_for_ratio(basename, ratio):
    return ["%s.%s_%s" % (basename, t, ratio) for t in ("main", "delta")]

from contextlib import nested

def create_ratio(filename, ratio):
    #lines = 
    nth = int(1/ratio)
    print ratio, nth
    main, delta = filenames_for_ratio(filename, nth)
    with nested(open(filename), open(main, "w"), open(delta, "w")) as (f, m, d):
        for _ in range(4):
            f.readline()
        lineno = 0
        for line in f:
            if lineno == 0:
                d.write(line)
            else:
                m.write(line)
            lineno += 1
            if lineno == nth:
                lineno = 0
    print subprocess.check_output(["wc", "-l", main])
    print subprocess.check_output(["wc", "-l", delta])
    return main, delta 


def main():
    global server


    fname = "order_line.tbl"
    setup(fname)
    for sf in SCALES:
        d = "sf%s" % sf
        for ratio in RATIOS:
            main, delta = create_ratio(d+"/"+fname, ratio)
            nth = extract_ratio(main)
            server = Server()
            for layout_name, layout in HEADERS.iteritems():
                tablename = "orderline_%s_%s" % (nth, layout_name)
                load(main, delta, tablename, layout)
                create_index(tablename, 0)
                create_index(tablename, 3)
                for papi in ["PAPI_TOT_INS"]:
                    x = (values[0]() - 11) % 3000
                    y = values[3](sf-1)
                    
                    for scan_t, scan_f in SCAN_TYPES.iteritems():
                        combo_str = "sf%s/%s/%s/%s/%s" % (sf, layout_name, nth, scan_t, papi)
                        print combo_str
                        for i in range(RUNS):
                            if scan_t.endswith("_range"):
                                print x, x+10, y, y+1
                                query(tablename, {0: [x, x+10], 3: [y, y+1]}, scan_f, papi, combo_str)
                            else:
                                query(tablename, {0: x, 3: y}, scan_f, papi, combo_str)
            delete(main)
            delete(delta)
                        
main()
                         
