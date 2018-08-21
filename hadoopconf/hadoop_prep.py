from xml.dom.minidom import parse
import sys

if len(sys.argv) < 4: 
    print("ERROR: Insufficient number of input arguments. Expected 2") 
    sys.exit(1)    

core_site = sys.argv[1] # /mnt/vhs/sparkpilot//hadoop-2.7.7/etc/hadoop/core-site.xml 
yarn_site = sys.argv[2] #'/mnt/vhs/hadoop-2.7.7/etc/hadoop/yarn-site.xml'

with parse(yarn_site) as yarnconf:
    prop = yarnconf.getElementsByTagName("property")[1]
    nn_value = yarnconf.createElement("value")
    nn_value.appendChild(yarnconf.createTextNode(sys.argv[3]))

    prop.replaceChild(nn_value, prop.getElementsByTagName("value")[0])
    
    with open(yarn_site, "w") as outf:
        yarnconf.writexml(outf)    
  
with parse(core_site) as hadoopconf:
    prop = hadoopconf.getElementsByTagName("property")[0]
    nn_value = hadoopconf.createElement("value")
    nn_value.appendChild(hadoopconf.createTextNode('hdfs://' + sys.argv[3] + ':9000'))

    prop.replaceChild(nn_value, prop.getElementsByTagName("value")[0])

    with open(core_site, "w") as outf:
        hadoopconf.writexml(outf)
