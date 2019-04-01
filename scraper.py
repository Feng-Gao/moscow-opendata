#!/usr/bin/python3

#a quick and dirty script to scrape/harvest resource-level metadata records from data.gov.sg
#the original purpose of this work is to support the ongoing international city open data index project led by SASS

import requests
import datetime
import time
from bs4 import BeautifulSoup
import scraperwiki

#moscow provides a platform level api
api_url = 'https://apidata.mos.ru/v1/datasets?api_key=366f5a5b04f505a54f5d580ac9809a01&$inlinecount=allpages&foreign=true&'
result = requests.get(api_url)
package_list = result.json()['Items']

#statics we need
#total dataset we process
p_count = 1
#total archived dataset we processed. This is unique on moscow as its api outputs also archived datasets
archive_count = 0

# iterate each dataset
for p in package_list:
    package_id = p['Id']
    package_url_id = p['SefUrl']
    package_isarchive = p['IsArchive']
    if package_isarchive:
        archive_count += 1
    package_created = p['PublishDate'] if p['PublishDate'] else 'BLANK'
    package_updated = p['LastUpdateDate'] if p['LastUpdateDate'] else 'BLANK'

    package_detail_url ="https://apidata.mos.ru/v1/datasets/"+str(package_id)+"?api_key=366f5a5b04f505a54f5d580ac9809a01"
    result = requests.get(package_detail_url)
    package_detail = result.json()

    #moscow's offical api offers dataset details information which contains name,org,desc,topics,tags, created, updated, row_num and column info
    package_name = '"'+package_detail['Caption']+'"'
    print("the  "+str(p_count)+" dataset is " + package_name)
    package_org = '"'+package_detail['DepartmentCaption']+'"' if package_detail['DepartmentCaption']!= "" else package_detail["orgName"]
    #replace quotes with star and linebreaker with double space in the scraped description text for easy store in csv
    package_desc = '"'+package_detail['Description'].replace('"',"*").replace("\n","  ")+'"' if package_detail['Description'] else 'BLANK'
    #may have multiple topics
    package_topics = '"'+package_detail['CategoryCaption']+'"' if package_detail.get('CategoryCaption','')!= '' else 'MISSING'
    #may have multiple topics or keyword is null
    package_tags = '"'+package_detail['Keywords']+'"' if package_detail['Keywords'] else 'BLANK'

    package_row_num = package_detail['ItemsCount']
    package_column_num = len(package_detail['Columns'])

    #moscow's metadata page has json export valid through internal apiproxy which offers information on update frequency and historical dataset
    #for weired reason, for some dataset, the meta.json is missing
    package_meta_url = "https://data.mos.ru/apiproxy/opendata/"+package_url_id+"/meta.json"
    result = requests.get(package_meta_url)
    try:
        package_meta = result.json()
        package_format = package_meta['Format']
        package_frequency = '"'+package_meta['ProvenanceEng']+'"'if package_meta.get('ProvenanceEng','') else 'MISSING'
        #the Data key contains full update history we need to record the total num of updates, and each update date
        package_history = package_meta['Data']
        package_history_num = len(package_history)
        package_history_list = []
        package_history_diff = []
        for h in package_history:
            package_history_list.append(h['Created'])
        for x,y in zip(package_history_list[:-1],package_history_list[1:]):
            x_datetime = datetime.datetime(*time.strptime(x,'%d.%m.%Y')[0:3])
            y_datetime = datetime.datetime(*time.strptime(y,'%d.%m.%Y')[0:3])
            #calculate the timedifference between two updates. the unit is days
            history_diff = (x_datetime - y_datetime).total_seconds()/86400
            package_history_diff.append(history_diff)
        package_history_list = '"'+'|'.join(package_history_list)+'"'
        if package_history_list != []:
            package_history_average_update = int(sum(package_history_diff)/len(package_history_diff))
        else:
            #no update history then mark average update as -1
            package_history_average_update = 0
    except Exception as ex:
        print(ex)
        package_frequency = 'MISSING'
        package_history_num = 0
        package_history_list = 'MISSING'
        package_history_average_update = 0

    #regarding the download count, view count they are available only on webpage
    #update frequency may also be parsed from the page if it's missing in the meta.json
    package_page_url = "https://data.mos.ru/opendata/"+package_url_id
    result = requests.get(package_page_url)
    soup = BeautifulSoup(result.content,features="lxml")
    try:
        package_view_count,package_download_count = soup.find_all(attrs={'class':'count'})[0:2]
        package_view_count = package_view_count.text
        package_download_count = package_download_count.text
        if package_frequency == 'MISSING':
            package_frequency = soup.find_all('tr')[12].find_all('td')[1].span.text
    #as for archive it may not have valid page for parsing the two count nums
    except Exception as ex:
        print(ex)
        package_view_count = 0
        package_download_count = 0

    #package detail + resource detail as one row. write it into file as csv
    row = package_url_id+','+package_name+','+package_desc+','+package_org+','+package_topics \
            +','+package_tags+','+package_format+','+package_created+','+package_frequency+','+package_updated \
            +','+str(package_row_num)+','+str(package_column_num)+','+str(package_history_num)+','+package_history_list\
            +','+str(package_history_average_update)+','+str(package_view_count)+','+str(package_download_count)+','+str(package_isarchive)+'\n'
    scraperwiki.sqlite.save(unique_keys=['package_url_id'],data={
                                    "package_url_id":package_url_id,
                                    "name": package_name, 
                                    "description": package_desc,
                                    "org":package_org,
                                    "topics":package_topics,
                                    "tags":package_tags,
                                    "format":package_format,
                                    "created":package_created,
                                    "frequency":package_frequency,
                                    "updated":package_updated,
                                    "row_num":package_row_num,
                                    "column_num":package_column_num,
                                    "history_num":package_history_num,
                                    "history_list":package_history_list,
                                    "history_average":package_history_average_update,
                                    "view_count":package_view_count,
                                    "download_count":package_download_count,
                                    "isarchive":package_isarchive,
                                                                
                                    })

    print('****************end---'+package_name+'---end****************')
    p_count += 1
#close the file
print("total dataset is "+str(p_count)+" ,and total archive is "+str(archive_count))
csv_file.close()
