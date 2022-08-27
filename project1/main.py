try: 
    import requests
    from datetime import datetime
    from bs4 import BeautifulSoup
    import pandas as pd
    import os
    import logging
    import threading
    
except ModuleNotFoundError:
    os.system('pip install requests')
    os.system('pip install pandas')
    os.system('pip install beautifulsoup4')
    os.system('pip install datetime')

alp = list(map(chr, range(97, 123)))
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def deleteDuplicates():
    filename = 'exports/URLQueue.csv'
    df = pd.read_csv(filename)
    df.drop_duplicates(inplace = True)
    df.to_csv(filename)
    return 

def deleteBalise(string):
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
    print(string)
    return string

def updateRowTemp(Newrow):
    filename = 'exports/URLQueue.csv'
    df = pd.DataFrame(Newrow)
    df.to_csv(filename,mode='a',index=False,header=False)
    return

def SongURLUpdate(URLRow):
    filename = 'exports/SongURLs.csv'
    df = pd.DataFrame(URLRow, index=[0])
    df.to_csv(filename,mode='a',index=False,header=False)
    return

def SongURLStatus(url):
    filename = 'exports/SongURLs.csv'
    df = pd.read_csv(filename)
    index = df.index[df["SongUrl"]== url].tolist()
    df.loc[index,['ScrapeStatus']] = 'Done'
    df.to_csv(filename, index=False)
    print(index)
    return

def SingerStatus(queueLink):
    filename = 'exports/URLQueue.csv'
    df = pd.read_csv(filename)
    index = df.index[df["Link"]== queueLink].tolist()
    df.loc[index,['ScrapeStatus']] = 'Done'
    df.to_csv(filename, index=False)
    return



def GettingIndividualSingerLinks(total, Alphabet):
    try:
        l = []
        t = 1
        while t <= int(total):
            url = f"https://www.hindigeetmala.net/singer/{Alphabet}.php?page={t}"
            req = requests.get(url)
            bsObj = BeautifulSoup(req.text, "html.parser")
            singers = bsObj.find_all("td", class_ = "w25p h150")
            bsObj1 = BeautifulSoup(str(singers), "html.parser")
            singersList = bsObj1.find_all("a", href=True)
            for j in singersList:
                url2 = 'https://www.hindigeetmala.net/' +j['href'][1:]
                l.append(url2)
            t+=1
        
        Newrow = {"Alphabet":Alphabet, 'Link':l, "ScrapeStatus": 'NotDone'}
        updateRowTemp(Newrow)
        
        return
    
    except Exception as e: print(e)
    
def UrlLinksOrder():
    try:
        for i in range(0,len(alp)):
            url = f"https://www.hindigeetmala.net/singer/{alp[i]}.php"
            req = requests.get(url)
            bsObj = BeautifulSoup(req.text, "html.parser")
            pages = bsObj.find_all("td", class_ = "alcen w720 bg7f")
            pages = str(pages[0])
            ind = pages.find('Page')
            if ind != -1:
                s1 = slice(ind+9, ind+12)
                total = pages[s1]
                ind1 = total.find('<')
                if ind1 != -1: 
                    s2 = slice(ind1,)
                    total = total[s2]
            else: 
                total = 1
            Alphabet = alp[i]

            
            GettingIndividualSingerLinks(total, Alphabet)
       
        return
    except Exception as e: print(e)
    
def SingerSongs():
    filename = 'exports/URLQueue.csv'
    df = pd.read_csv(filename)
    queue = df.loc[df['ScrapeStatus'] == 'NotDone']
    data = queue.to_numpy()
    for i in range(len(data)): 
        queueLink = data[i][2]
        SingerSongsPages(queueLink)
    return

def SingerSongsPages(queueLink):
    url = queueLink
    req = requests.get(url)
    bsObj = BeautifulSoup(req.text, "html.parser")
    pages = bsObj.find_all("td", class_ = "alcen w720 bg7f")
    pages = str(pages[0])
    ind = pages.find('Page')
    if ind != -1:
        s1 = slice(ind+9, ind+15)
        total = pages[s1]
        ind1 = total.find('<')
        if ind1 != -1: 
            s2 = slice(ind1,)
            total = total[s2]
    else: 
        total = '1'
    SingerTotalSongs(total, queueLink)
    return

def SingerTotalSongs(total, queueLink):
    t = 1 
    print(total)
    while t <= int(total):
        url = queueLink + f'?page={t}'
        req = requests.get(url)
        bsObj = BeautifulSoup(req.text, "html.parser")
        songsList = bsObj.find_all("table", class_ = "b1 w760 bgff pad2 allef")
        bsObj = BeautifulSoup(str(songsList), "html.parser")
        songsList = bsObj.find_all('a', itemprop = 'url')
        bsObj = BeautifulSoup(str(songsList), "html.parser")
        songsList = bsObj.find_all('a', href = True)
        m =[]
        for k in songsList:
           url2 = 'https://www.hindigeetmala.net/' +k['href'][1:]
           m.append(url2)
        for i in range(0,len(m)):
            if '/song' in m[i]:
                URLRow = {'SongUrl': m[i], 'ScrapeStatus' :'NotDone'}
                SongURLUpdate(URLRow)
        t += 1 
    SingerStatus(queueLink)
    return 

def SongDetails():
    filename = 'exports/SongURLs.csv'
    df = pd.read_csv(filename)
    queue = df.loc[df['ScrapeStatus'] == 'NotDone']
    data = queue.to_numpy()
    for i in range(0,len(data)):
        url = data[i][0]
        SongMetaData(url)
    return

def SongMetaData(url):
    req = requests.get(url)
    bsObj = BeautifulSoup(req.content, "html.parser")
    lyrics =  bsObj.find_all("span", itemprop="text")
    youtube = bsObj.find_all("iframe", src= True) 
    songData = bsObj.find_all("table", class_ = "b1 w760 bgff pad2 allef")
    SongDetailsF = bsObj.find_all("table", class_ ="b1 allef w100p")
    bsObj = BeautifulSoup(str(songData), 'html.parser')
    songData = bsObj.find_all('tr')
    bsObj = BeautifulSoup(str(songData), 'html.parser')
    thumbnail = bsObj.find_all('img', src = True) 
    names = bsObj.find_all(itemprop ='name') 
    genre = bsObj.find_all(itemprop ='genre')
    artist = bsObj.find_all(itemprop ='byArtist')
    artobj = BeautifulSoup(str(artist), 'html.parser')
    artistN = artobj.find_all(itemprop = 'name')
    composer = bsObj.find_all(itemprop ='composer')
    composerobj = BeautifulSoup(str(composer), 'html.parser')
    composerN = composerobj.find_all(itemprop = 'name')
    album = bsObj.find_all(itemprop ='inAlbum')
    albumobj = BeautifulSoup(str(album), 'html.parser')
    albumN = albumobj.find_all(itemprop = 'name')
    year = albumobj.find_all('a')
    lyric = bsObj.find_all(itemprop ='lyricist')
    lyricobj = BeautifulSoup(str(lyric), 'html.parser')
    lyricN = lyricobj.find_all(itemprop = 'name')
    direcobj0 = BeautifulSoup(str(SongDetailsF), 'html.parser')
    director = direcobj0.find_all(itemprop ='director')
    direcobj = BeautifulSoup(str(director), 'html.parser')
    DirN = direcobj.find_all(itemprop = 'name')
    producer = direcobj0.find_all(itemprop ='producer')
    prodobj = BeautifulSoup(str(producer), 'html.parser')
    prodN = prodobj.find_all(itemprop = 'name')
    cast = direcobj0.find_all(itemprop ='actor')
    castobj = BeautifulSoup(str(cast), 'html.parser')
    castN = castobj.find_all(itemprop = 'name')
    actors = bsObj.find_all('td',class_ ='w150')
    actobj = BeautifulSoup(str(actors), 'html.parser')
    actN = actobj.find_all('span', itemprop ='name')
    
    if len(names) != 0: 
        songName = str(names[0])
        for i in range(2):
            # identifying  <
            rankBegin = 0
            for carac in songName:
                if carac == '<':
                    break
                rankBegin += 1
            # identifying  >
            rankEnd = 0
            for carac in songName:
                if carac == '>':
                    break
                rankEnd += 1
            stringToReplace = songName[rankBegin:rankEnd+1]
            songName = songName.replace(stringToReplace,'')
    else: 
        songName = 'SongName'

    if len(lyrics) != 0:
        lyrics = str(lyrics[0])
        for i in range(6):
            # identifying  <
            rankBegin = 0
            for carac in lyrics:
                if carac == '<':
                    break
                rankBegin += 1
            # identifying  >
            rankEnd = 0
            for carac in lyrics:
                if carac == '>':
                    break
                rankEnd += 1
            stringToReplace = lyrics[rankBegin:rankEnd+1]
            lyrics = lyrics.replace(stringToReplace,'')
    else: 
        lyrics = 'No lyrics available for this song'   
         
    if len(DirN) != 0: 
        DirectorName = str(DirN[0])
        for i in range(2):
            # identifying  <
            rankBegin = 0
            for carac in DirectorName:
                if carac == '<':
                    break
                rankBegin += 1
            # identifying  >
            rankEnd = 0
            for carac in DirectorName:
                if carac == '>':
                    break
                rankEnd += 1
            stringToReplace = DirectorName[rankBegin:rankEnd+1]
            DirectorName = DirectorName.replace(stringToReplace,'')
    else: 
        DirectorName = 'No Director Details Available'
    
    if len(albumN) != 0: 
        albumName = str(albumN[0])
        for i in range(2):
            # identifying  <
            rankBegin = 0
            for carac in albumName:
                if carac == '<':
                    break
                rankBegin += 1
            # identifying  >
            rankEnd = 0
            for carac in albumName:
                if carac == '>':
                    break
                rankEnd += 1
            stringToReplace = albumName[rankBegin:rankEnd+1]
            albumName = albumName.replace(stringToReplace,'')
    else:
        albumName = 'AlbumName'

    genreList = []
    for i in range(len(genre)):
        string = str(genre[i])
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
        genreList.append(string)
    
    artistList = []
    for i in range(len(artistN)):
        string = str(artistN[i])
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
        artistList.append(string)
    
    composerList = []
    for i in range(len(composerN)):
        string = str(composerN[i])
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
        composerList.append(string)
    
    lyricList = []
    for i in range(len(lyricN)):
        string = str(lyricN[i])
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
        lyricList.append(string)
    
    if len(prodN) != 0:
        producerList = []
        for i in range(len(prodN)):
            string = str(prodN[i])
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
            producerList.append(string)
    else: 
        producerList = 'No Producer Details Available'

    if len(castN) != 0:
        castList = []
        for i in range(len(castN)):
            string = str(castN[i])
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
            castList.append(string)
    else: 
        castList = 'No Cast Details available'
    if len(thumbnail) != 0:
        for t in thumbnail:
            url2 = 'https://www.hindigeetmala.net/' +t['src'][1:]
        thumbnailL = url2
    else:
        thumbnailL = 'Not Available'   

    if len(youtube) == 0:
        youtubeURL = 'No youtube link available'
    else: 
        for y in youtube:
            youtubeURL = y['src']

    if len(year)!=0:
        year = year[0]
        year = str(year)
        albumYear = year[-9:-5]
    else:
        albumYear = 'NotAvailable'

    if len(castList) != 0: 
        Actors = castList[0]
    else:
        Actors = 'No Cast Details available'
        castList = 'No Cast Details available'

    MasterUpdate(songName,lyrics,DirectorName,albumName,genreList,artistList,composerList,lyricList,thumbnailL,youtubeURL,albumYear,Actors,producerList,castList)
    SongURLStatus(url)
    
    return
    
def MasterUpdate(songName,lyrics,DirectorName,albumName,genreList,artistList,composerList,lyricList,thumbnailL,youtubeURL,albumYear,Actors,producerList,castList):
    filename = 'exports/Master3.csv'
    row = [{"Song Title" : songName,
       "Thumbnail": thumbnailL,
       "Singer": artistList,
       "Music Director": composerList,
       "Lyricist": lyricList,
       "Actor": Actors,
       "Category": genreList,
       "Youtube": youtubeURL,
       "Lyrics": lyrics,
       "Movie": albumName,
       "Year": albumYear,
       "Movie Director": DirectorName,
       "Producers" : producerList,
       "Cast" : castList
    }]
    df = pd.DataFrame(row, index=[0])
    df.to_csv(filename,mode='a',index=False,header=False)
    return

if __name__ == '__main__':
    start_time = datetime.now()
    LOGGER.info('Started Website Scrapping')
    print(start_time)
    UrlLinksOrder()
    LOGGER.info('Completed URL Queue')
    print(datetime.now())
    LOGGER.info('Started Duplication Check')
    deleteDuplicates()
    print(datetime.now())
    LOGGER.info('Completed Duplication Check')
    LOGGER.info('Started Individual Songs Scrapping')
    SingerSongs()
    print(datetime.now())
    SongDetails()
    LOGGER.info('Completed Website Scrapping')
    end_time = datetime.now()
    print(end_time)
    

    