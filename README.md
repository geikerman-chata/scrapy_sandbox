# scrapy_sandbox

Collection of python scripts to scrape TripAdvisor by leveraging their robot.txt sitemap. 

WARNING: The code is usable, albeit wobbly. There are many nice-to-haves to still implement, efficiencies to gain and silly multi-processing work-arounds that this repo is begging for.    

## Initialization

Clone this repo to a linux machine (most testing done on Debian 10) ensure the local repo holds the scrapy.cfg (scrapy config file).
```bash 
git clone https://github.com/geikerman-chata/scrapy_sandbox
```

The following bash commands will install wget, chrome, chromdriver and other programs to support chromedriver running smoothly (jdk & jre):
```bash 
$ sudo apt install wget
$ sudo apt-get update
$ sudo apt-get install -y curl unzip xvfb libxi6 libgconf-2-4
$ sudo apt-get install default-jdk 
$ sudo apt install default-jre
$ wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
$ sudo apt install ./google-chrome-stable_current_amd64.deb
$ sudo apt install chromium-driver
```

In the working directory install the requirements using pip:
```bash 
$ pip install requirements.txt -r
```

## Quick Start

To download the trip advisor sitemap xmls and initialized the input folder run:
```bash
$ python3 download_hotel_xml.py
```
To run one spider for example:
```bash
$ python3 spidermother.py -f 0 -s 0 -b 0
```

NOTE: The spider's print a lot of output for debugging purposes. It's recommended that you wrap the commands to run a spider or series of spiders with nohup & which will force it to run in background (you will be able to close the vm ssh, and it will still be running) and store the command line output to a file, like this:

```bash
$ nohup python3 spidermother.py -f 0 -s 0 -b 0 &> cmdline.out &
```

This command run 1 spider using xml file 0 (of 35) as input. The spider will keep running until all urls in the specified xml have been visited. 

The inputs here are a bit cryptic: 
There are 35 Trip Advisor xml zip files each with ~50,000 hotel urls.  The numbers in square brackets is the range of acceptable values following each argument:

-f [0 - 35] The xml file number to use as input for the spider. 

-s [0 - 49999] Which index in the xml file do you want to spider to start on.

-b [any int] Do you want to save to google buckets. 

WARNING:
 If -b is left out as a parameter, the output is saved locally in "cwd"/output. 
Hope you have plenty of hard-drive space in your local repo. 

To run multiple spiders in parallel:
```bash
$ python3 spider_control.py -s 0 -f 8 - i 1000
```
The above command would run 8 spiders in parallel. Their input date is sourced from xml files 0-7 (8 is the non-inclusive upper bound) and start at url index 1000 (out of 50,000 in the xml files)

Again the arguments are cryptic and a little inconsistent with spidermother.py:

-s [0 - 35]
Lower inclusive bound of the xml file number to use as input for the spider. Must e less than -f [int]

-f [0 - 35] Upper non-inclusive bound of the xml file number to use as input for the spider. Must be greater that -s [int]

-i [0 - 49999] Index in all of active input xmls (-s [int] to -f [int]) where the spiders will start. 

-n [any int] Only include if you want files saved on the local machine instead of the google cloud bucket. 


## More info

A google cloud platform virtual machine (16 vcpu + 16GB ram) is comfortable running 8 spiders at once, like the following command:
```bash 
spider_control.py -s 0 -f 8 - i 1000
``` 
This setup + command will produce an average of 5,000 English reviews (with english responses) per hour from one vm.

There is a problem with running more than 8 spiders on a virtual machine however, and it seems to be related to a clash between networking / bucket access and multiprocessing.
The processes don't crash, but output is very slow. Network throughput halves from ~2 MiB/sec to ~1 MiB/sec and the data output from the spiders is drastically reduced.

There are other programs in the repo that are usesful diagnostic tools or were used to "encourage" program to change its output format. 
```python
monitor.py :
```
Very simple diagnostic tool for reading the marker.txt files in the xml input folder to try and guage overall progress. And doesn't work very well.
The marker.txt files are competency substitutes for a real queue or stack, which would orchestrate input urls for multiple spiders (for multiprocessing)
 ```python            
data_rollup.py & complile.py:
```
Tools to "roll-up" individual review files stored in google buckets into larger json files. 



