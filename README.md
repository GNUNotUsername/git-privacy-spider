# GPS: The Git Privacy Spider

Everyone has seen and laughed at some poor student/intern/senior developer who has committed his auth tokens to a publicly visible repo. You might have even done it yourself! As such, many security papers have been written examining the prevalence of such blunders and their potential security implications if not rectified. I took this idea one step further.

In my final few weeks as a teaching assistant I discovered that many students accidentally revealed their home addresses by submitting assessment pieces containing photos which were taken on their mobile phones. In many cases, these photos contained the immediate GPS position of that phone within their EXIF data.

This is a spider which crawls all across public repos and scans for GPS metadata. The results are compiled into a MariaDB database and can be exported into a CSV.

# NOTE: This is for research purposes only. DO NOT BE EVIL PLEASE!

# Usage
To initiate a crawling session:

```sudo python gps.py <count>```

where ```<count>``` is the number of repos to examine in this session.

To export the current results into a CSV:

```sudo python gps.py <path>```

where ```<path>``` is the path to the CSV to export.

# Dependencies:
* Python >3.10
* MariaDB >= 11.2
* SQLAlchemy >=1.4
* SQLAlchemy-Utils
* python-mysqlclient
* python-requests >=2.1
* perl-image-exiftool >=12.70
