# Overview
An internal reporting tool that generates informative summaries from a news database.

This project satisfies a project requirement for Udacity's [Full Stack Web Developer Nanodegree](https://www.udacity.com/course/full-stack-web-developer-nanodegree--nd004).

# Enviroment Setup

### Install the virtual machine

1. Download & install  [VirtualBox](https://www.virtualbox.org/wiki/Downloads)

2. Download & install [Vagrant](https://www.vagrantup.com/downloads.html)
Check if Vagrant is successfully installed by running on terminal.

  ```vagrant --version```

  If you see the version number, then you're good.

### Download VM configuration

1. Download or Clone the repository
```git clone https://github.com/kiraheta/logs-analysis-project.git```

3. ```cd``` into directory **FSND-Virtual-Machine**. Inside, you will find another directory called **vagrant**. Change directory into the vagrant directory

### Running the VM
1. Inside the vagrant subdirectory, run the command
```vagrant up```. Doing so will download & install the Linux OS.

2. Once ```vagrant up``` is completed and your shell prompt returns, run ```vagrant ssh``` to log into newly installed Linux OS.

3. Lastly, run ```cd /vagrant```

### Download the data
1. Download & unzip [newsdata.zip](https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip)

2. Place the file ```newsdata.sql``` inside of the ```vagrant``` directory

3. Load the data via the command
```psql -d news -f newsdata.sql```

## Running the internal reporting tool

1. Run ```python log.py```

## Program output

If module runs successfully, you will see the following output:

![output](https://github.com/kiraheta/logs-analysis-project/blob/master/output.png)
