try: 
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd
    import os
    import logging
    from datetime import datetime
    import json 
    import itertools
except ModuleNotFoundError:
    os.system('pip install requests')
    os.system('pip install pandas')
    os.system('pip install beautifulsoup4')
    os.system('pip install datetime')
    
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def deleteDuplicates():
    filename = 'project2/exports/AlbumURLQueue.csv'
    df = pd.read_csv(filename)
    dup = df.drop_duplicates(inplace = True)
    df.to_csv(filename)
    return 

def updateCombined(Newrow):
    filename = 'project2/exports/CombinedQueue.csv'
    df = pd.DataFrame(Newrow)
    df.to_csv(filename,mode='a',index=False,header=False)
    return

def deleteBalise(string):
    for i in range(7):
        # identifying  <
        rankBegin = 0
        for carac in string:
            if carac == '<':
                break
            rankBegin += 1
        # identifying  >
        rankEnd = 0
        for carac in string:
            if carac == '>':
                break
            rankEnd += 1
        stringToReplace = string[rankBegin:rankEnd+1]
        string = string.replace(stringToReplace,'')
    return string

def getAlbumCombinedLinks():
    
    f = open('project2/exports/temp/ref.json')
    data = json.load(f)
    f.close()

    genre_exact = ["Rock", "Electronic","Pop", "Folk%2C+World%2C+%26+Country","Jazz", 
                "Funk+%2F+Soul", "Classical","Hip+Hop","Latin","Reggae","Stage+%26+Screen","Blues",
                "Non-Music","Children%27s","Brass+%26+Military"]
        
    country_exact = ["US","UK","Germany","France","Italy","Europe","Netherlands","Japan","Canada","Spain",
        "Unknown","Sweden","Australia","Russia","Belgium","Greece","Jamaica","Brazil","Finland","Poland",
        "USSR","Switzerland","Denmark","Norway","Mexico","Austria","UK+%26+Europe","Yugoslavia",
        "Czechoslovakia","Argentina","Portugal","Hungary","USA+%26+Canada","Czech+Republic","Romania",
        "Turkey","New+Zealand","India","Taiwan","Colombia","Ireland","Ukraine","German+Democratic+Republic+%28GDR%29",
        "Chile","South+Africa","Venezuela","Israel","Peru","Bulgaria","South+Korea","Scandinavia","Germany%2C+Austria%2C+%26+Switzerland",
        "Croatia","Hong+Kong", "Europe+%26+US","Indonesia","Malaysia","Lithuania","USA+%26+Europe","Iceland",
        "Serbia","Slovakia","UK+%26+Ireland","Slovenia","UK+%26+US","Benelux","Thailand","Middle+East","China",
        "Estonia","Egypt","Worldwide","Singapore","Lebanon","Cuba","Ecuador","Philippines","USA%2C+Canada+%26+Europe",
        "Nigeria","Uruguay","Puerto+Rico","Australia+%26+New+Zealand","Bolivia","Latvia","Trinidad+%26+Tobago","Panama",
        "Australasia","Barbados","Belarus","USA%2C+Canada+%26+UK","Luxembourg","Pakistan","Iran","Serbia+and+Montenegro",
        "Macedonia","Bosnia+%26+Herzegovina","Costa+Rica","Kenya","Ghana","North+America+%28inc+Mexico%29",
        "Czech+Republic+%26+Slovakia","Zaire","France+%26+Benelux","Ivory+Coast","Guatemala","Dominican+Republic",
        "El+Salvador","Morocco","Algeria","Germany+%26+Switzerland","Angola","Singapore, Malaysia+%26+Hong+Kong","Reunion",
        "Madagascar","Zimbabwe","Cyprus","Congo, Democratic+Republic+of+the","Tunisia","United+Arab+Emirates","Azerbaijan"]
    

    decade = ["2020","2010","2000","1990","1980","1970","1960","1950","1940","1930","1920","1910","1900","1890","1860"]
    a = [genre_exact,country_exact,decade]

    if data['Combined'] == 'NotDone':
        combined = list(itertools.product(*a))
        i =0
        while i < len(combined):
            url = f"https://www.discogs.com/search/?limit=250&layout=sm&type=master&genre_exact={combined[i][0]}&country_exact={combined[i][1]}&decade={combined[i][2]}"
            Newrow = [{'CombinedURl': url, 'Status':'NotDone'}]
            updateCombined(Newrow)
            i+=1
        f = open('project2/exports/temp/ref.json','w+')
        data['Combined'] = 'Done'
        f.seek(0)
        json.dump(data,f)
        f.truncate()
        f.close()

    else: 
        print('Skipping Combined Process')

    return

def GettingAlbumLinks(queueLink):
    try:
        t = 1
        while t < 100:
            l = []
            url = f"{queueLink}&page={t}"
            print(url)
            req = requests.get(url)
            bsObj = BeautifulSoup(req.text, "html.parser")
            albums = bsObj.find_all("li", role = "listitem")
            bsObj1 = BeautifulSoup(str(albums), "html.parser")
            albumList = bsObj1.find_all("a", href=True)
            bsObj2 = BeautifulSoup(str(albums), "html.parser")
            albumList = bsObj2.find_all("a", class_ ='search_result_title' )
            for j in albumList:
                url2 = 'https://www.discogs.com/' +j['href'][1:]
                l.append(url2)
            labelN = bsObj1.find_all('p', class_="card_info")
            
            lt = 0 

            while lt < 250: 
                country = []
                label = []
                labelM = BeautifulSoup(str(labelN[lt]), "html.parser")
                countryN = labelM.find_all("span", class_='card_release_country')
                labelA = labelM.find_all('a', href = True)
                
                if len(countryN) == 1: 
                    string = str(countryN[0])
                    coun = deleteBalise(string)
                    country.append(coun)
                    
                elif len(countryN) == 0:
                    country = ''
                    
                else: 
                    ctl = 0
                    call = []
                    while ctl < len(countryN):
                        string = str(countryN[ctl])
                        coun = deleteBalise(string)
                        call.append(coun)
                        
                        ctl+=1
                    country.append(call)
                    

                if len(labelA) == 1: 
                    string = str(labelA[0])
                    lab = deleteBalise(string)
                    label.append(lab)
                    

                elif len(labelA) == 0:
                    label = ''

                else: 
                    ltl = 0
                    lball = []
                    while ltl < len(labelA):
                        string = str(labelA[ltl])
                        lab = deleteBalise(string)
                        lball.append(lab)
                        ltl+=1
                    label.append(lball)
                    
                Newrow = {'Link':l[lt], "ScrapeStatus": 'NotDone', 'Country': country, 'Label' : label}
                updateRowTemp(Newrow)
                lt+=1
            
            t+=1
            
        return
    except Exception as e: print(e)

def updateRowTemp(Newrow):
    filename = 'project2/exports/AlbumURLQueue.csv'
    df = pd.DataFrame(Newrow)
    df.to_csv(filename,mode='a',index=False,header=False)
    return


def AlbumURLStatus(queueLink):
    filename = 'project2/exports/AlbumURLQueue.csv'
    df = pd.read_csv(filename)
    index = df.index[df["Link"]== queueLink].tolist()
    df.loc[index,['ScrapeStatus']] = 'Done'
    df.to_csv(filename, index=False)
    return


def AlbumQueue():
    filename = 'project2/exports/AlbumURLQueue.csv'
    df = pd.read_csv(filename)
    queue = df.loc[df['ScrapeStatus'] == 'NotDone']
    data = queue.to_numpy()
    for i in range(len(data)): 
        queueLink = data[i][0]
        country = data[i][2]
        label = data[i][3]
        AlbumMetaData(queueLink,country,label)
    return


def AlbumMetaData(queueLink,country,label):
    req = requests.get(queueLink)
    bsObj = BeautifulSoup(req.content, "html.parser")
    album = bsObj.find_all('h1', class_='title_1q3xW')
    albumobj = BeautifulSoup(str(album), 'html.parser')
    artistN = albumobj.find_all('a')
    thumbnail = bsObj.find_all('a', class_ = 'link_1ctor thumbnail_32ntz')
    thumobj = BeautifulSoup(str(thumbnail), 'html.parser')
    thumbnail = thumobj.find_all('img', src = True)
    genre = bsObj.find_all('table', class_ ='table_1fWaB')
    genreobj = BeautifulSoup(str(genre), 'html.parser')
    genre = genreobj.find_all('td')
    credits =  bsObj.find_all("ul", class_="container_3h-8D")
    Credobj = BeautifulSoup(str(credits), 'html.parser')
    credits= Credobj.find_all('a', class_ = 'link_1ctor link_15cpV')
    statics = bsObj.find_all('div', class_='container_2DsE3')
    statobj = BeautifulSoup(str(statics), 'html.parser')
    statics = statobj.find_all('a')
    avg = statobj.find_all('span')
    playlist = bsObj.find_all('button',class_='video_SuWuh')
    playobj = BeautifulSoup(str(playlist), 'html.parser')
    playNames = BeautifulSoup(str(playlist), 'html.parser')
    playlist = playobj.find_all('img', src = True)
    playListNames = playNames.find_all('div', class_='title_26yzZ')
    
    string = str(artistN[0])
    artistName = deleteBalise(string)

    albumName = str(album[0])
    albumName = deleteBalise(albumName)
    artistNam = artistName+'*'
    albumName = albumName.replace(artistNam,'')
    albumName = albumName.replace('–','')
  
    genreList = []
    for i in range(len(genre)):
        string = str(genre[i])
        for i in range(18):
        # identifying  <
            rankBegin = 0
            for carac in string:
                if carac == '<':
                    break
                rankBegin += 1
            # identifying  >
            rankEnd = 0
            for carac in string:
                if carac == '>':
                    break
                rankEnd += 1
            stringToReplace = string[rankBegin:rankEnd+1]
            string = string.replace(stringToReplace,'')
        genreList.append(string)
   
    Genre = genreList[0]
    Style = genreList[1]
    Year = genreList[2]
    
    satisticList = []
    for i in range(len(statics)):
        string = str(statics[i])
        for i in range(8):
        # identifying  <
            rankBegin = 0
            for carac in string:
                if carac == '<':
                    break
                rankBegin += 1
            # identifying  >
            rankEnd = 0
            for carac in string:
                if carac == '>':
                    break
                rankEnd += 1
            stringToReplace = string[rankBegin:rankEnd+1]
            string = string.replace(stringToReplace,'')
        satisticList.append(string)

    Have = satisticList[0]
    Want = satisticList[1]
    Ratings = satisticList[2]

    string = str(avg[3])
    avg = deleteBalise(string)
    avg = avg[:4]

    if len(credits) != 0:
        creditsL = []
        for i in range(len(credits)):
            string = str(credits[i])
            for i in range(10):
            # identifying  <
                rankBegin = 0
                for carac in string:
                    if carac == '<':
                        break
                    rankBegin += 1
                # identifying  >
                rankEnd = 0
                for carac in string:
                    if carac == '>':
                        break
                    rankEnd += 1
                stringToReplace = string[rankBegin:rankEnd+1]
                string = string.replace(stringToReplace,'')
            creditsL.append(string)
    else: 
        creditsL = 'No Credit Details Available'


    if len(playListNames) != 0:
        IndYout = []
        for i in range(len(playListNames)):
            string = str(playListNames[i])
            for i in range(2):
            # identifying  <
                rankBegin = 0
                for carac in string:
                    if carac == '<':
                        break
                    rankBegin += 1
                # identifying  >
                rankEnd = 0
                for carac in string:
                    if carac == '>':
                        break
                    rankEnd += 1
                stringToReplace = string[rankBegin:rankEnd+1]
                string = string.replace(stringToReplace,'')
            IndYout.append(string)
    else: 
        IndYout = []
    
    for t in thumbnail:
        url2 = t['src']
    thumbnail = url2

    youtubeURL = []
    if len(playlist)!= 0: 
        for yt in playlist:
            url2 = yt['src']
            url2 = url2[23:]
            url2 = url2[:-12]
            yurl = f"https://youtu.be/{url2}"
            youtubeURL.append(yurl)

    label = label
    country = country
    
    if len(youtubeURL) == len(IndYout): 
        p = 0
        while p < len(youtubeURL):
            vtitle = IndYout[p]
            vid = youtubeURL[p]
            MasterUpdate(artistName,albumName,Genre,Style,Year,Have,Want,Ratings,avg,creditsL,vtitle,vid,thumbnail,label,country)
            p+=1
    AlbumURLStatus(queueLink)
    
    return
    
def MasterUpdate(artistName,albumName,Genre,Style,Year,Have,Want,Ratings,avg,creditsL,vtitle,vid,thumbnail,label,country):
    filename = 'project2/exports/Master.csv'
    row = [{
       "Album" : albumName,
       "Thumbnail": thumbnail,
       "Artists": artistName,
       "Genre": Genre,
       "Style": Style,
       "Year": Year,
       "Have": Have,
       "Want": Want,
       "Ratings": Ratings,
       "Avg Ratings": avg,
       "Credits": creditsL,
       "Video Title": vtitle,
       "Video ID" : vid,
       "Label" : label,
       "Country" : country
    }]
    df = pd.DataFrame(row, index=[0])
    df.to_csv(filename,mode='a',index=False,header=False)
    return

def main():
    st = datetime.now()
    print("start time:",st)
    
    f = open('project2/exports/temp/ref.json')
    data = json.load(f)
    f.close()

    getAlbumCombinedLinks()

    if data['URL Scrape'] == 'NotDone':
        filename = 'project2/exports/CombinedQueue.csv'
        df = pd.read_csv(filename)
        queue = df.loc[df['Status'] == 'NotDone']
        Data = queue.to_numpy()
        for i in range(len(Data)): 
            queueLink = Data[i][0]
            GettingAlbumLinks(queueLink)

        f = open('project2/exports/temp/ref.json','w+')
        data['URL Scrape'] = 'Done'
        f.seek(0)
        json.dump(data,f)
        f.truncate()
        f.close()
    else:
        print('Skipping URL Scrapping')

    AlbumQueue()
    et = datetime.now()
    print("End time:", et)

if __name__ == '__main__':
    main()
    