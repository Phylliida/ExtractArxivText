# ExtractArxivText
Extract only the text of all arxiv source files, for archival purposes (I wanted to put them on a few archival CDs)

1. Follow https://github.com/brienna/arxiv and put your aws credentials in config.ini
2. Run

```
python3 extractor.py
```

and it will download all arxiv files and extract only the text.

Initially it uses internet archive's files which are free to download,
but those are out of date by a few years. So once it finishes downloading those,
the rest are then downloaded using amazon s3.

Total size should be about 290 GB, compressed.

Feel free to modify 

```
textFileTypes
```

to include more types of files.

Currently, all files of types within `textFileTypes` are included,
all other file types are removed.