import os
from eodag import EODataAccessGateway
from eodag import setup_logging
from shapely import wkt
import shutil
import zipfile
import yaml
import collections
import re
import subprocess
#import datetime as DT
from datetime import datetime
import glob
import numpy as np
import time
from xml.dom import minidom
import json
import geojson
from osgeo import gdal

main_folder = 'E:/PathoSAT/'
extension = ".zip"

def search(location,start_date,end_date):
    name = location.split('.')[0]
    #edit the YAML file
    with open("C:/Users/vasiloudisg/.config/eodag/eodag.yml", "r") as file:
        # Load the YAML file into a Python dictionary
        data = yaml.safe_load(file)
    data["onda"]["download"]["outputs_prefix"] = "{}{}".format(main_folder,name)
    data["mundi"]["download"]["outputs_prefix"] = "{}{}".format(main_folder,name)
    # data["theia"]["download"]["outputs_prefix"] = "{}{}".format(main_folder,name)
    # data["ecmwf"]["api"]["outputs_prefix"] = "{}{}".format(main_folder,name)
    # data["cop_ads"]["api"]["outputs_prefix"] = "{}{}".format(main_folder,name)
    # data["usgs"]["api"]["outputs_prefix"] = "{}{}".format(main_folder,name)
    
    # Save the changes to the YAML file
    with open("C:/Users/vasiloudisg/.config/eodag/eodag.yml", "w") as file:
        yaml.dump(data, file)
        
    dag = EODataAccessGateway()
    setup_logging(2)
        
    wkt_f = "{}locations/{}".format(main_folder,location)
    with open(wkt_f, 'r') as f:
        wkt_f = f.readlines() #works ONLY if geojson is written in one line!
        geojson = wkt_f[0]
        geojson = geojson.split('[[')[1].split(']]')[0]
        geojson = geojson.replace('[','')
        geojson = geojson.replace(']','')
        geojson = geojson.split(',')
        new_input = "POLYGON(({} {},{} {},{} {},{} {},{} {}))".format(geojson[0],geojson[1],geojson[2],geojson[3],geojson[4],geojson[5],geojson[6],geojson[7],geojson[8],geojson[9])
        
            
    wkt_f = wkt.loads(new_input)
        
    
    #dag.set_preferred_provider("onda") #use this if not priorities
        
    search_results = dag.search_all(
        productType='S2_MSI_L1C', #S1A_IW_GRDH
        geom=wkt_f, 
        start='{}T00:00:10.153Z'.format(start_date),
        end='{}T23:59:10.153Z'.format(end_date)
        )
        
    print("Total products for {} found: {}".format(name,len(search_results)))
    print(" ")
    
    if location == "aksiosZthessaloniki.geojson":
        print("T34TFL tile code is faulty for river {}. Removing it...".format(location.split('Z')[0]))
        for product in search_results:
            if 'T34TFL' in str(product):
                search_results.remove(product)
        print("Total products after removal of faulty tile: {}".format(len(search_results)))
    # for defChantecoq use R051_T31UFP for correct tile
    elif location == "derChantecoq.geojson":
        print("R051_T31UFP is the correct tile code for lake {}. Removing the other one...".format(location.split('Z')[0]))
        for product in search_results:
            if not 'R051_T31UFP' in str(product):
                search_results.remove(product)
        print("Total products after removal of faulty tile: {}".format(len(search_results)))
    # for orestiadaZkastoria use R093_T34TEK for correct tile
    elif location == "orestiadaZkastoria.geojson":
        print("_R093_T34TEK_ is the correct tile code for lake {}. Removing the other one...".format(location.split('Z')[0]))
        for product in search_results:
            if not '_R093_T34TEK_' in str(product):
                search_results.remove(product)
        for product in search_results:
            if not '_R093_T34TEK_' in str(product): #to fix the bug
                search_results.remove(product)
        print("Total products after removal of faulty tile: {}".format(len(search_results)))
    # for iskarDamZsofia use R093_T34TGN for correct tile
    elif location == "iskarDamZsofia.geojson":
        print("R093_T34TGN is the correct tile code for lake {}. Removing the other one...".format(location.split('Z')[0]))
        for product in search_results:
            if not 'R093_T34TGN' in str(product):
                search_results.remove(product)
        for product in search_results:
            if not 'R093_T34TGN' in str(product):
                search_results.remove(product)
        print("Total products after removal of faulty tile: {}".format(len(search_results)))
    # for bellenZjonkoping use R022_T33VWD for correct tile
    elif location == "bellenZjonkoping.geojson":
        print("R022_T33VWD is the correct tile code for lake {}. Removing the other one...".format(location.split('Z')[0]))
        for product in search_results:
            if not 'R022_T33VWD' in str(product):
                search_results.remove(product)
        print("Total products after removal of faulty tile: {}".format(len(search_results)))
    # for pancharevoDamZsofia use R093_T34TFN for correct tile
    elif location == "pancharevoDamZsofia.geojson":
        print("R093_T34TFN is the correct tile code for lake {}. Removing the other one...".format(location.split('Z')[0]))
        for product in search_results:
            if not 'R093_T34TFN' in str(product):
                search_results.remove(product)
        print("Total products after removal of faulty tile: {}".format(len(search_results)))
    # for salihorskayeZsalihosrsk use R093_T35UNU for correct tile
    elif location == "salihorskayeZsalihosrsk.geojson":
        print("R093_T35UNU is the correct tile code for lake {}. Removing the other one...".format(location.split('Z')[0]))
        for product in search_results:
            if not 'R093_T35UNU' in str(product):
                search_results.remove(product)
        print("Total products after removal of faulty tile: {}".format(len(search_results)))
    # for beliIskarDamZsofia use R093_T34TGM for correct tile
    elif location == "beliIskarDamZsofia.geojson":
        print("R093_T34TGM is the correct tile code for lake {}. Removing the other one...".format(location.split('Z')[0]))
        for product in search_results:
            if not 'R093_T34TGM' in str(product):
                search_results.remove(product)
        print("Total products after removal of faulty tile: {}".format(len(search_results)))
    # for verligkaZlakmos use R093_T34TEK for correct tile
    elif location == "verligkaZlakmos.geojson":
        print("R093_T34TEK is the correct tile code for lake {}. Removing the other one...".format(location.split('Z')[0]))
        for product in search_results:
            if not 'R093_T34TEK' in str(product):
                search_results.remove(product)
        print("Total products after removal of faulty tile: {}".format(len(search_results)))
    
    return search_results


def download(location,start_date, end_date,search_results):
    name = location.split('.')[0]
    folder_name = "{}{}".format(main_folder,name)
    # for Aksios river the correct tile code is T34TFK, the other one is missing the lake
    
        
    if len(search_results) == 0:
        print("No products found.")
        print("Make sure mundi API key is not expired!")
    else:
        first_tile_code = str(search_results[0]).split('_')[5]
        #second_tile_code = str(search_results[-1]).split('_')[5]
        for result in search_results:
            if str(result).split('_')[5] != first_tile_code:
                second_tile_code = str(result).split('_')[5]
            else:
                second_tile_code = ''
        
        first_tile_products = []
        second_tile_products = []
        for product in search_results:
            if first_tile_code in str(product):
                first_tile_products.append(product)
            else:
                second_tile_products.append(product)
        if len(first_tile_products) == 1:
            print("{} product with tile code: {}".format(len(first_tile_products),first_tile_code))
        else:
            print("{} products with tile code: {}".format(len(first_tile_products),first_tile_code))
        if second_tile_code =='':
            print("There is no second tile products.")
        else:
            if len(second_tile_products)==1:
                print("{} product with tile code: {}".format(len(second_tile_products),second_tile_code))
            else:
                print("{} products with tile code: {}".format(len(second_tile_products),second_tile_code))
        
        #search_results = second_tile_products
        #Choose the tile with more products to download
        if(len(first_tile_products)>=len(second_tile_products) and len(second_tile_products)==0):
            chosen_tile_products = first_tile_products
            if len(chosen_tile_products) == 1:
                print("{} product will be downloaded with tile code: {}".format(len(chosen_tile_products),first_tile_code))
            else:
                print("{} products will be downloaded with tile code: {}".format(len(chosen_tile_products),first_tile_code))
        else:
            chosen_tile_products = second_tile_products
            if len(chosen_tile_products) == 1:
                print("{} product will be downloaded with tile code2: {}".format(len(chosen_tile_products),second_tile_code))
            else:
                print("{} products will be downloaded with tile code2: {}".format(len(chosen_tile_products),second_tile_code))
        
        
        dag = EODataAccessGateway()
        setup_logging(2)
        #Download the products 
                           #chosen_tile_products 
        dag.download_all(chosen_tile_products, providers =["onda"]) #make sure to change download location in eodag.yml #providers =["mundi","onda"]
            
        
        
        #Remove .downloaded folder
        if os.path.exists("{}/{}/.downloaded".format(main_folder,name)): #remove folder .downloaded if exists
            shutil.rmtree("{}/{}/.downloaded".format(main_folder,name))
            print(".downloaded folder removed")
        else:
            print(".downloaded folder does not exist")
        
        
        
        # #remove files that are not folders. (SAFEs failed to download)
        # files = [os.path.join(folder_name, f) for f in os.listdir(folder_name) if not os.path.isdir(os.path.join(folder_name, f))]
        # for file in files:
        #     if not file.endswith(".tif"): #avoid remove older tif in that folder
        #         os.remove(file) 
          
        #unzip if zips exist
        for item in os.listdir(folder_name): # loop through items in dir
            if item.endswith(extension): # check for ".zip" extension
                file_name = f"{folder_name}/{item}" # get full path of files
                zip_ref = zipfile.ZipFile(file_name) # create zipfile object
                zip_ref.extractall(folder_name) # extract file to dir
                zip_ref.close() # close file
                os.remove(file_name) # delete zipped file
        
        
        #Mundi provider download SAFE files as folders without the ending .SAFE
        #read all folders and add .SAFE to all of them!
        for item in os.listdir(folder_name):
            if not item.endswith(".SAFE") and not item.endswith(".tif"):
                os.rename("{}/{}".format(folder_name,item),"{}/{}.SAFE".format(folder_name,item))
        
        #Remove duplicates SAFEs to prepare for R!
        def getFolderSize(folder):
            total_size = os.path.getsize(folder)
            for item in os.listdir(folder):
                itempath = os.path.join(folder, item)
                if os.path.isfile(itempath):
                    total_size += os.path.getsize(itempath)
                elif os.path.isdir(itempath):
                    total_size += getFolderSize(itempath)
            return total_size
        
        files = os.listdir(folder_name)
        safes = []
        for file in files:
            if file.endswith(".SAFE"):
                safes.append(file)
    
    
        dates = []
        for safe in safes:
            date = safe.split('_')[2]
            dates.append(date)
    
        
        duplicates = [item for item, count in collections.Counter(dates).items() if count > 1]
        for duplicate in duplicates:
            matching_date_safes = [s for s in safes if duplicate in s]
            first = matching_date_safes[0]
            second = matching_date_safes[1]
            size_of_first = getFolderSize("{}/{}".format(folder_name,first))
            size_of_second = getFolderSize("{}/{}".format(folder_name,second))
            if size_of_first <= size_of_second:
                print("Removing {} with size of: {}".format(first,size_of_first))
                shutil.rmtree("{}/{}".format(folder_name,first))
            else:
                print("Removing {} with size of: {}".format(second,size_of_second))
                shutil.rmtree("{}/{}".format(folder_name,second))


def check_download(location):
    location = location.split(".")[0]
    files = os.listdir(f"E:\PathoSAT\{location}")
    faulty_date = ""
    faulty_download = ""
    for file in files:
        if file.endswith(".SAFE") and not os.path.isdir(f"E:\PathoSAT\{location}\{file}"):
            faulty_download = file
            faulty_date_temp = faulty_download.split("_")[2].split("T")[0]
            faulty_date = datetime.strptime(faulty_date_temp, '%Y%m%d').strftime('%Y-%m-%d')
            
    #remove the faulty_file if exists
    if len(faulty_download)>1:
        os.remove(f"E:\PathoSAT\{location}\{faulty_download}")
        
    return faulty_date,faulty_date



def move_safes(location):
    #create "old_safes" folder to keep the old .SAFEs there
    try:
        os.mkdir("{}old_safes".format(main_folder))
    except:
        print("{}old_safes folder already exists!".format(main_folder))
    
    folder_name = location.split(".")[0] #remove .geojson

    try:
        os.mkdir("{}old_safes/{}".format(main_folder,folder_name))
    except:
        print("{}old_safes/{} folder already exists!".format(main_folder,folder_name))
    #Get the ols .SAFES   
    all_files = os.listdir(main_folder+folder_name)
    safes = []
    for file in all_files:
        if file.endswith(".SAFE"):
            safes.append(file)
    if len(safes) > 0:
        #move safes to old_safes/location_name
        for safe in safes:
            source = '%s%s/%s' % (main_folder,folder_name,safe)
            destination = '%sold_safes/%s/%s' % (main_folder,folder_name,safe)
            shutil.move(source,destination)
        print("Succesfully moved %s .SAFES from %s folder" % (len(safes),folder_name))
        print("------------")
    else:
        print("No .SAFEs in folder %s" % folder_name)


def return_safes(location):
    folder_name = location.split(".")[0] #remove .geojson
    try:
        safes = os.listdir(main_folder+'old_safes/'+folder_name)
        if len(safes) > 0:
            for safe in safes:
                source = '%sold_safes/%s/%s' % (main_folder,folder_name,safe)
                destination = '%s%s/%s' % (main_folder,folder_name,safe)
                shutil.move(source,destination)
            print('%s .SAFEs have been returned succesfully' % len(safes))
        else:
            print("No .SAFEs in folder old_safes/%s" % folder_name)
        shutil.rmtree('%sold_safes/%s' % (main_folder,folder_name))
    except:
        print("No old .SAFEs to be returned for %s" % folder_name)

def move_tifs(location):
    #create "old_tifs" folder to keep the old .SAFEs there
    try:
        os.mkdir("{}old_tifs".format(main_folder))
    except:
        print("{}old_tifs folder already exists!".format(main_folder))
    folder_name = location.split(".")[0] #remove .geojson
    try:
        os.mkdir("{}old_tifs/{}".format(main_folder,folder_name))
    except:
        print("{}old_tifs/{} folder already exists!".format(main_folder,folder_name))
    #Get the ols .SAFES   
    all_files = os.listdir(main_folder+folder_name)
    tifs = []
    for tif in all_files:
        if tif.endswith(".tif"):
            tifs.append(tif)
    if len(tifs) > 0:
        #move tifs to old_tifs/location_name
        for tif in tifs:
            source = '%s%s/%s' % (main_folder,folder_name,tif)
            destination = '%sold_tifs/%s/%s' % (main_folder,folder_name,tif)
            shutil.move(source,destination)
        print("Succesfully moved %s .tifs from %s folder" % (len(tifs),folder_name))
        print("------------")
    else:
        print("No .tifs in folder %s" % folder_name)


def return_tifs(location):
    folder_name = location.split(".")[0] #remove .geojson
    try:
        tifs = os.listdir(main_folder+'old_tifs/'+folder_name)
        if len(tifs) > 0:
            for tif in tifs:
                source = '%sold_tifs/%s/%s' % (main_folder,folder_name,tif)
                destination = '%s%s/%s' % (main_folder,folder_name,tif)
                shutil.move(source,destination)
            print('%s .tifs have been returned succesfully' % len(tifs))
        else:
            print("No .tifs in folder old_tifs/%s" % folder_name)
        shutil.rmtree('%sold_tifs/%s' % (main_folder,folder_name))
    except:
        print("No old .tifs to be returned for %s" % folder_name)
        
            
        
def check_dates(start_date, end_date):
    today = datetime.now()
    #check if the date given is not future date
    start_date_date = datetime.strptime(start_date, '%Y-%m-%d') #convert to date
    end_date_date = datetime.strptime(end_date, '%Y-%m-%d') #convert to date
    if start_date_date > today or end_date_date > today:
        print("Cannot have future data!")
        start = False
    elif start_date_date > end_date_date:
        print("Wrong Dates given!")
        start = False
    else:
        print("Correct Dates given!")
        start = True
    return start_date,end_date,start



def rename_tifs(location):
    folder_name = location.split(".")[0]
    tifs = [f for f in os.listdir("{}{}".format(main_folder,folder_name)) if re.match(r'.*_atm_corrected.tif', f)]
    for tif in tifs:
        new_tif_name = tif.replace(".tif","_%s.tif"%folder_name)
        if tif.endswith("_atm_corrected.tif"):
            if os.path.exists("{}{}/{}".format(main_folder,folder_name,new_tif_name)):
                print("File already exists. Deleting existing file...")
                os.remove("{}{}/{}".format(main_folder,folder_name,new_tif_name))
            os.rename("%s%s/%s"%(main_folder,folder_name,tif),"%s%s/%s"%(main_folder,folder_name,new_tif_name))

def move_geojsons(location):
    geojsons = [f for f in os.listdir("{}locations".format(main_folder)) if re.match(r'.*.geojson', f)]
    for i in geojsons:
        if i != location:
            shutil.move("{}locations/{}".format(main_folder,i),"{}all_locations/temp_locations/{}".format(main_folder,i))
    print("Moved all %s geojsons to /temp_locations except %s" % (len(geojsons)-1,location))
    
def return_geojsons():
    geojsons = [f for f in os.listdir("{}all_locations/temp_locations".format(main_folder)) if re.match(r'.*.geojson', f)]
    for i in geojsons:
        shutil.move("{}all_locations/temp_locations/{}".format(main_folder,i),"{}/locations/{}".format(main_folder,i))
    print("Returned %s geojsons to /locations" % len(geojsons))

def countTifs(location):
    tifs = glob.glob(f"E:/PathoSAT/{location}"+"/"+'*.tif')
    return len(tifs)

def countSafes(location):
    safes = glob.glob(f"E:/PathoSAT/{location}"+"/"+'*.SAFE')
    return len(safes)

def runR():
    print("Starting R...")
    #Call the R script 'unity.R' and display its output in real-time
    r_script = "D:/PathoSAT/new_unity.R"
    r_path = "C:/Program Files/R/R-4.2.2/bin/Rscript.exe"
    r_lib_path = "C:/Users/vasiloudisg/AppData/Local/R/win-library/4.2" #R libraries
    os.environ["R_LIBS_USER"] = r_lib_path #Set the R_LIBS_USER environment variable to the path of your R libraries
    
    process = subprocess.Popen([r_path, r_script], stdout=subprocess.PIPE)
    while True:
        output = process.stdout.readline()
        if output == b'' and process.poll() is not None:
            break
        if output:
            print(output.strip())
    print("Finished with R.")
    




def create_metadata(location,j,download_link):
    main_folder = 'E:/PathoSAT/'
    nameZ = location.split(".")[0]  
    
    tifs = [filename for filename in os.listdir(os.path.join(main_folder, nameZ)) if filename.endswith("_atm_corrected.tif")]
    
    
    # Create the json values
    productID = re.sub("_atm_corrected.tif", "", tifs[j])
    platformName = re.sub("_.*", "", productID)
    productType = re.sub("_20.*", "", productID)
    productType = re.sub("._", "", productType)
    productType = re.sub(productType[5], "", productType)
    sensingDate = productID.split("_")[2]
    lake_name = nameZ.split("Z")[0]
    city_name = nameZ.split("Z")[1]
    outputLocalServerPath = f"algaemap-chl_{city_name}_{lake_name}_{sensingDate}-PathoCERT.tif"
    
    id = lake_name.upper()[:3]
    id = f"{id}_ALG"
    
    xml_file_name = productID.split("_")[1]
    
    os.rename(os.path.join(main_folder, nameZ, tifs[j]), os.path.join(main_folder, nameZ, outputLocalServerPath))
    

    
    wd = f"E:/PathoSAT/{nameZ}/"
    chl_mean = get_pixel_mean(wd + outputLocalServerPath)
    
    
    safe_of_tifs = [re.sub("_atm_corrected.tif", ".SAFE", filename) for filename in tifs]
    xml_path = os.path.join(wd, safe_of_tifs[j], f"MTD_{xml_file_name}.xml")
    xmldataframe = minidom.parse(xml_path)
    clouds_percentage = xmldataframe.getElementsByTagName("Cloud_Coverage_Assessment")[0].firstChild.nodeValue
    clouds_percentage = round(float(clouds_percentage), 4)
    
    tile_coordinates_list = xmldataframe.getElementsByTagName("EXT_POS_LIST")[0].firstChild.nodeValue
    
    # Convert string to numpy array
    coords = np.fromstring(tile_coordinates_list, sep=' ').reshape(-1, 2)
    reversed_coords = reversed_coords = coords[::-1]
    swapped_coords = [[row[1], row[0]] for row in reversed_coords]
    rounded_array = [[round(value, 6) for value in sublist] for sublist in swapped_coords]

   
    productBbox = {
        "type": "Polygon",
        "coordinates": [rounded_array]
    }
    
    geojson_path = os.path.join(main_folder, "locations", f"{nameZ}.geojson")
    with open(geojson_path) as f:
        json_file = geojson.load(f)
    coords = json_file["features"][0]["geometry"]["coordinates"]
    
    processedBbox = {
        "type": "Polygon",
        "coordinates": coords
    }
    
    statical_mean = {
        "measureName": "pixels means",
        "value": chl_mean,
        "unitOfMeasure": "mg/m^3",
        "suggestedThreshold1": 10,
        "suggestedThreshold2": 50,
        "suggestedThreshold3": 100
    }
    
    cloudInfo = {
        "value": clouds_percentage,
        "suggestedThreshold1": 30,
        "suggestedThreshold2": 70
    }
    
   
    productURL = download_link
    
    final_json_file = {
        "productID": productID,
        "productURL": productURL,
        "productBbox": productBbox,
        "sensingDate": sensingDate,
        "productFormat": "SAFE",
        "outputLocalServerPath": outputLocalServerPath,
        "storageFormat": "GeoTIFF",
        "productType": productType,
        "platformName": platformName,
        "processedBbox": processedBbox,
        "statisticalValues": statical_mean,
        "cloudInfo": cloudInfo,
        "id": id,
        "pilot": city_name,
        "provider": "pathocert/CERTH",
        "analysisType": "algaemap-chl"
    }
    
    
    def convert_to_python_types(data):
        if isinstance(data, dict):
            return {key: convert_to_python_types(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [convert_to_python_types(item) for item in data]
        elif isinstance(data, np.floating):
            return float(data)
        elif isinstance(data, np.integer):
            return int(data)
        elif isinstance(data, np.ndarray):
            return data.tolist()
        else:
            return data


    with open(os.path.join(main_folder, "sync_files/metadata", f"{id}_{sensingDate}.json"), "w") as file: #export to json
        final_json_file_converted = convert_to_python_types(final_json_file)
        json.dump(final_json_file_converted, file, indent=4)


    
    
    
    image_dest = os.path.join(main_folder, "sync_files/images")
    shutil.copy(os.path.join(wd, outputLocalServerPath), os.path.join(image_dest, outputLocalServerPath))
    time.sleep(1)
    os.rename(os.path.join(main_folder, nameZ, outputLocalServerPath), os.path.join(main_folder, nameZ, tifs[j]))
    time.sleep(1)


def get_pixel_mean(file_path):
    # Open the raster dataset
    dataset = gdal.Open(file_path)
    # Read the image as a numpy array
    data = dataset.ReadAsArray()
    # Exclude negative values
    data = np.where(data >= 0, data, np.nan)
    # Calculate the mean pixel value
    mean_pixel_value = np.nanmean(data)
    rounded_value = format(mean_pixel_value, '.4f')
    return float(rounded_value)
