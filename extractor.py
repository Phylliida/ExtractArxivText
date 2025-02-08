# start with internet archive which is free to download from, only goes to a few years ago

from internetarchive import get_item
import time
import os
import requests
from tqdm import tqdm


# mopdified from https://github.com/brienna/arxiv
import boto3, configparser
from bs4 import BeautifulSoup
s3resource = None

def setup():
    """Creates S3 resource & sets configs to enable download."""

    # Securely import configs from private config file
    configs = configparser.SafeConfigParser()
    configs.read('config.ini')

    # Create S3 resource & set configs
    global s3resource
    s3resource = boto3.resource(
        's3',  # the AWS resource we want to use
        aws_access_key_id=configs['DEFAULT']['ACCESS_KEY'],
        aws_secret_access_key=configs['DEFAULT']['SECRET_KEY'],
        region_name='us-east-1'  # same region arxiv bucket is in
    )


import boto3, configparser, os, botocore

def download_file(key, targetPath):
    """
    Downloads given filename from source bucket to destination directory.

    Parameters
    ----------
    key : str
        Name of file to download
    """

    # Ensure src directory exists 
    if not os.path.isdir('src'):
        os.makedirs('src')
    
    if os.path.exists(targetPath):
        print("Already download:" + key)
        return
    # Download file
    print('\nDownloading s3://arxiv/{} to {}...'.format(key, key))

    try:
        s3resource.meta.client.download_file(
            Bucket='arxiv', 
            Key=key,  # name of key to download from
            Filename=targetPath,  # path to file to download to
            ExtraArgs={'RequestPayer':'requester'})
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print('ERROR: ' + key + " does not exist in arxiv bucket")



 




def get_all_items(collection_name):
    items = []
    rows = 1000000000  # Max rows per request
    # we just request all rows since doing start only goes up to 10000
    # Construct the API URL for the collection
    url = (
        "https://archive.org/advancedsearch.php?"
        f"q=collection%3A%22{collection_name}%22"
        f"&rows={rows}"
        "&output=json"
        "&fl[]=identifier"
    )
    print(url)
    response = requests.get(url)
    time.sleep(1.0)
    data = response.json()
    
    docs = data.get('response', {}).get('docs', [])
    # Collect item identifiers
    items.extend(doc['identifier'] for doc in docs)
    print("Got " + str(len(items)))        
    return items
import gzip

def download_file_arxiv(url, save_path):
    """Download a file with a progress bar."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Get total file size from headers (if available)
        total_size = int(response.headers.get('content-length', 0))
        
        # Initialize progress bar
        progress_bar = tqdm(
            desc=os.path.basename(save_path),
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024
        )
        
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                progress_bar.update(len(chunk))  # Update progress bar
        
        progress_bar.close()
        print(f"Downloaded: {save_path}")
    
    except Exception as e:
        print(f"Failed to download {url}: {str(e)}")
        if 'progress_bar' in locals():
            progress_bar.close()

def extract_tar(tar_path, extract_dir):
    """Extract a .tar file to a directory."""
    try:
        with tarfile.open(tar_path, 'r') as tar:
            # Create extraction directory if it doesn't exist
            os.makedirs(extract_dir, exist_ok=True)
            tar.extractall(path=extract_dir)
        print(f"Extracted {tar_path} to {extract_dir}")
    except tarfile.TarError as e:
        print(f"Failed to extract {tar_path}: {str(e)}")
        
import shutil
def unzip_gz(gz_file, output_dir):
    if os.path.exists(output_dir):
        print(f"already unzipped {output_dir}")
        return
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get the original filename without .gz extension
    output_filename = os.path.basename(gz_file)
    if output_filename.endswith('.gz'):
        output_filename = output_filename[:-3]
    
    # Construct full output path
    output_path = os.path.join(output_dir, output_filename)
    
    # Unzip the file
    with gzip.open(gz_file, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        
import tarfile
def extract_tar_gz(tar_gz_path, extract_dir):
    if os.path.exists(extract_dir):
        print(f"already unzipped {extract_dir}")
        return
    """Extract a .tar.gz file with progress tracking."""
    try:
        # Check if the file is valid
        if not tarfile.is_tarfile(tar_gz_path):
            unzip_gz(tar_gz_path, extract_dir)
            return
        with tarfile.open(tar_gz_path, 'r:gz') as tar:
            members = tar.getmembers()
            os.makedirs(extract_dir, exist_ok=True)
            
            # Progress bar for extraction
            with tqdm(total=len(members), 
                    desc=f"Extracting {os.path.basename(tar_gz_path)}",
                    unit="file") as pbar:
                for member in members:
                    tar.extract(member, path=extract_dir)
                    pbar.update(1)
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(e)
        

import json
collectionList = "collection_items.json"
if not os.path.exists(collectionList):
    item_names = sorted(get_all_items("arxiv-bulk"))
    import json
    with open(collectionList, "w") as f:
        json.dump(item_names, f)

with open(collectionList, "r") as f:
    item_names = json.load(f)
    print(f"Got {len(item_names)} items")
    
    
textFileTypes = [
    'tex', 'ltx', 'sty', 'doc', 'latex', # tex source file
    'texauxil', # auxillary tex data (like bib)
    'cfg', # tex configs
    'bib', 'bst', 'bbx', 'refs', # bibliography
    'cls', 'def', 'dtx', 'clo', # formatting/document style
    'cld', # context lua documents
    'xml', # xml data
    'sty', # macros and extensions
    'fd', # font definition,
    'ind', # ?
    'ins', # installation to extract template files out of a dtx file
    'htm', 'html', # webpages
    'tab', 'gr',# table data
    'txt', # sometimes they store the source like this
    'abs', # abstract
    ]
nonTextFileTypes = [
    'pk', # media
    
    'graf', 'mf', 'eps', 'ep', 'fig', 'epsf', # text file for graph
    'pstex', # figures
    'gif', 'epsi', 'png', 'jpg', 'jpeg', 'bit', 'tfm', 'eps', #images
    # auto generated
    'aux', # temporary, not needed to store
    'bbl', # bib
    'blg', # bibtex and biber log file
    'bcf', # biblatex control file
    'toc', # table of contents
    'lof', # list of figures
    'lot', # list of tables
    'log', # log
    'pdf', # output pdf
    'nav', # navitation by hyperlinks
    'vrb', # verbatim material when fragile
    'snm', 'out', # beamer intermediate files
    'tuc', # ConTExt MkIV files
    't1','t2', 't3', 'f1a', 'f2a', 'f3a', 'f1b', 
    'ps', # output postscript (pdf-like format) file, sometimes figures are stored in this
    'postscript', # output postscript (sometimes figures, not enough space)
    'gz', # gzipped usually from .synctex.gz which is just syncronization
    'mp', # some tex autogenerated thing
    'lnk', 'xxx', # what u doin
]

nonTextFileTypes += [str(i) for i in range(50)] 
nonTextFileTypes += [f't{i}' for i in range(50)] # temporary .0 or .t4 things
nonTextFileTypes += [f'ps{i}' for i in range(50)] # autogenerated figures
nonTextFileTypes += [f'fig{i}' for i in range(50)] # figure raw data (too much space to store all of them)
nonTextFileTypes += [f'f{i}' for i in range(50)] # figure raw data (too much space to store all of them)


if __name__ == '__main__':
    """Runs if script is called on command line"""
    setup()
    # Download manifest file to current directory
    download_file('src/arXiv_src_manifest.xml', 'arXiv_src_manifest.xml')
    manifest = open('arXiv_src_manifest.xml', 'r')
    soup = BeautifulSoup(manifest, 'xml')

    # Print last time the manifest was edited
    timestamp = soup.arXivSRC.find('timestamp', recursive=False).string
    print("Manifest was last edited on " + timestamp)

    # Print number of files in bucket
    files = [x.filename.text.replace(".tar", "") for x in soup.find_all('file')]
    archiveFiles = set(['src/' + item for item in item_names])
    s3Files = set(files)
    nonArchiveFiles = s3Files - archiveFiles
    overlappingFiles = s3Files & archiveFiles
    print(list(s3Files)[0], list(archiveFiles)[0])
    print("Available additional files not in archive: " + str(len(nonArchiveFiles)))
    print("Overlapping files: " + str(len(overlappingFiles)))

       
    from collections import defaultdict
    item_names = sorted(item_names)
    extensions = defaultdict(lambda: [])
    for name in item_names + sorted(list(nonArchiveFiles)):
        if not "_src_" in name: continue # ignore pdf
        outDir = f'out/{name}'
        completedPath = f"{outDir}/completed.txt"
        completedDownloadPath = f"{outDir}/completeddownload.txt"
        completedExtractPath = f"{outDir}/completedextract.txt"
        downloadPath = f'{outDir}/{name}.tar'
        extractFolder = f'{outDir}/{name}'
        outZip = f'{outDir}/{name}.7z'
        if os.path.exists(completedPath):
            print("Completed:"+ name)
            continue
        if not os.path.exists(completedDownloadPath):
            from pathlib import Path
            Path(outDir).mkdir(parents=True, exist_ok=True)
            # s3
            if name.startswith("src/"):
                link = name + ".tar"
                download_file(link, downloadPath)
            else:
                link = f'https://archive.org/download/{name}/{name}.tar'
                download_file_arxiv(link, downloadPath)
            with open(completedDownloadPath, "w") as f:
                f.write("Done")
        if not os.path.exists(completedExtractPath):
            extract_tar(downloadPath, extractFolder)
            with open(completedExtractPath, "w") as f:
                f.write("done")
        for p in os.listdir(extractFolder):
            fullPath = os.path.join(extractFolder, p)
            if os.path.isdir(fullPath):
                for pInner in list(os.listdir(fullPath)):
                    if not pInner.endswith(".gz"): continue
                    fileName = ".".join(os.path.basename(pInner).split(".")[:-1])
                    print(fileName)
                    extract_tar_gz(os.path.join(fullPath, pInner), os.path.join(fullPath, fileName))
                for pInner in list(os.listdir(fullPath)):
                    pInnerFullDir = os.path.join(fullPath, pInner)
                    if os.path.isdir(pInnerFullDir):
                        files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(pInnerFullDir) for f in filenames]
                        for f in files:
                            if "." in f and os.path.isfile(f):
                                extension = f.split(".")[-1].lower()
                                if extension in textFileTypes:
                                    # save it
                                    pass
                                elif extension in nonTextFileTypes:
                                    os.remove(f)
                                    # delete it
                                    pass
                                else:
                                    os.remove(f)
                                    # what are u
                                    extensions[extension] = f.replace("\\", "/")
                            elif os.path.isfile(f) and not '.' in f: # sometimes pdfs are stored without extension in the zip
                                os.remove(f)
                    else:
                        os.remove(pInnerFullDir)
        import py7zr
        print("Compressing...")
        filters = [{"id": py7zr.FILTER_ARM}, {"id": py7zr.FILTER_LZMA2, "preset": 9}]
        with py7zr.SevenZipFile(outZip, 'w', filters=filters) as z:
            z.writeall(extractFolder)
        print("Done compression")
        print("Cleaning up tar...")
        os.remove(downloadPath)
        print("Cleaning up folder")
        shutil.rmtree(extractFolder)
        print("Done cleaning up")
        with open(completedPath, 'w') as f:
            f.write("done")
        os.remove(completedDownloadPath)
        os.remove(completedExtractPath)


    print(len(item_names))
        