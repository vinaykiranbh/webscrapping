
try: 
    from pytube import Playlist
    import googleapiclient.discovery
    from urllib.parse import parse_qs, urlparse
    from urllib.request import Request
    from urllib.request import urlopen as uReq
    from json import load
    import gspread
    import json
    import threading
    from gspread.cell import Cell
    import os
    import logging 
    
except ModuleNotFoundError:
    os.system('pip install pytube')
    os.system('pip install gspread')
    os.system('pip install google-api-python-client')
    os.system('pip install urllib')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

    
def YoutubePlaylistURLScrape(worksheet,worksheet1,spreadsheet):

    try: 
        i = 0
        LOGGER.info('Total playlists: %s',str(len(worksheet)))
        while i < len(worksheet):
            playlist = worksheet[i]['PlaylistURL']
            query = parse_qs(urlparse(playlist).query, keep_blank_values=True)
            playlist_id = query['list'][0]
            
            print('started')
            Title = playlist_id
            urls = []
            p = 0
            playlist_urls = Playlist(playlist)
          
            
            
            for url in playlist_urls.video_urls[:500]:
                print(url)
                query = parse_qs(urlparse(url).query, keep_blank_values=True)
                Video_id = query['v'][0]
                print(Video_id)
                urls.append(Video_id)
                p+=1
            
           
            urls = [*set(urls)]
            print(len(urls))

            worklist = spreadsheet.worksheets()
            SheetExist = 'False'
            
            for lis in range(len(worklist)):
                if Title in str(worklist[lis]):
                    
                    SheetExist = 'True'
                    sheet = str(worklist[lis])
                    sheetI = sheet.find('id:')
                    sheetID = sheet[sheetI+3:-1]
            if SheetExist != 'True': 
        
                sheet = spreadsheet.add_worksheet(title= Title, rows=500, cols=12)
                
                sheet = str(sheet)
                sheetI = sheet.find('id:')
                sheetID = sheet[sheetI+3:-1]
               
            Result = f"https://docs.google.com/spreadsheets/d/1iKFF0h5fPbYLCFnuBYJ4u0jsq8jE76vPn_GtI0NdyMQ/edit#gid={sheetID}"
            LOGGER.info('sheet: %s', Result)
            
            Wsheet = spreadsheet.worksheet(Title)
            cells = []
            headers = []
            headers.append(Cell(row=1, col=1, value='Playlist URL'))
            headers.append(Cell(row=1, col=2, value='Video URL'))
            headers.append(Cell(row=1, col=3, value='Title'))
            headers.append(Cell(row=1, col=4, value= 'Hashtags'))
            headers.append(Cell(row=1, col=5, value='SongName' ))
            headers.append(Cell(row=1, col=6, value='ArtistName'))
            headers.append(Cell(row=1, col=7, value='AlbumName'))
            headers.append(Cell(row=1, col=8, value='Likes'))
            headers.append(Cell(row=1, col=9, value='Views'))
            headers.append(Cell(row=1, col=10, value='Description'))
            #headers.append(Cell(row=1, col=11, value='Thumbnail'))
            head = Wsheet.update_cells(headers)
            print(head)
            LOGGER.info('Headers updated in this playlist')
            
            youtube1 = googleapiclient.discovery.build("youtube", "v3", developerKey = "AIzaSyCFtqDeaKF1VNCe0m8YX-TXbC20qnZRRdI")
            
            lip = 0
            cells = []
            while lip < len(urls): 
                print(lip)
                
                video_id = urls[lip]
                video_request=youtube1.videos().list(
                        part='statistics,snippet',
                        id= video_id
                    )
                  
                video_response = video_request.execute()
                
                LOGGER.info('Individual Scrapped')
                hashtags = []
                description = video_response['items'][0]['snippet']['description']
                for word in description.split():
                    if word[0] == '#':
                        hashtags.append(str(word))
                hashtag = ' '.join([str(item) for item in hashtags])
                LOGGER.info('Starting url scrape')
                ArtistsName = ''
                AlbumName = ''
                    
                if 'items' in video_response:
                    
                    if len(video_response['items']) != 0: 
                        if 'likeCount' in video_response['items'][0]['statistics']:
                            likes = video_response['items'][0]['statistics']['likeCount']
                        else:
                            likes = '0'
                        if 'viewCount' in video_response['items'][0]['statistics']:
                            views = video_response['items'][0]['statistics']['viewCount']
                        else:
                            views = '0'
                        LOGGER.info('Stastics Scrapped')  
                    else:
                        likes = '0'
                        views = '0'
                else:
                    likes = '0'
                    views = '0'
                
                if len(video_response['items'][0]['snippet']['title']) != 0: 
                    title = video_response['items'][0]['snippet']['title']
                else: 
                    title = ''
                 
                yurl = 'https://youtu.be/' + video_id 
                
                Row = int(lip)+2 
                cells.append(Cell(row=Row, col=1, value=playlist))
                
                cells.append(Cell(row=Row, col=2, value=yurl))
                
                cells.append(Cell(row=Row, col=3, value= title))
                
                cells.append(Cell(row=Row, col=4, value= hashtag))
                
                cells.append(Cell(row=Row, col=5, value= title))
                
                cells.append(Cell(row=Row, col=6, value= ArtistsName))
                
                cells.append(Cell(row=Row, col=7, value= AlbumName))
                
                cells.append(Cell(row=Row, col=8, value=likes))
                
                cells.append(Cell(row=Row, col=9, value=views))
                
                cells.append(Cell(row=Row, col=10, value=description))
                
                #cells.append(Cell(row=Row, col=11, value= thumbnail))

                lip+=1    
            LOGGER.info('Individual URLs Scrapped')
            Wsheet.update_cells(cells)
            cell = worksheet1.find(playlist)
            worksheet1.update_cell(cell.row, 3, Result)
            worksheet1.update_cell(cell.row, 2, 'Done')
            LOGGER.info('Job Done for playlist %s out of %s',str(i+1), str(len(worksheet)))
            i +=1
        
    except Exception as e:
	    print(f"exception {e}")
    

     

    
    
#def lambda_handler(event, context):
if __name__ == '__main__':
    
    gc = gspread.service_account(filename='project3/credentials.json')
    spreadsheet = gc.open_by_url("https://docs.google.com/spreadsheets/d/1iKFF0h5fPbYLCFnuBYJ4u0jsq8jE76vPn_GtI0NdyMQ")
    worksheet1 = spreadsheet.get_worksheet(0)
    worksheet = list(filter(lambda data: data['Status'] == 'NotDone', worksheet1.get_all_records()))
    #Threading process
    master = threading.Thread(target=YoutubePlaylistURLScrape, args=(worksheet, worksheet1, spreadsheet))
    master.start()
    master.join()
    #YoutubePlaylistURLScrape(worksheet,worksheet1,spreadsheet)
    LOGGER.info('Completed Execution')
        
    
    