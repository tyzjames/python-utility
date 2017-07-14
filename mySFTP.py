# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 10:54:28 2017
Just a wrapper around the pysftp package
@author: james.toh
"""

import pysftp
import os
import re

class mySFTP:
    def __init__(self, hostname, username, password):
        self.hostname = hostname
        self.username = username
        self.password = password

    def connect(self, keyfile = None):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        if keyfile is not None:
            try:
                cnopts.hostkeys.load(keyfile)
            except:
                print("Error loading keyfile. Not attempting connection")
                return
        try:
            self.sftp =  pysftp.Connection(self.hostname, username=self.username, password=self.password, cnopts=cnopts)
        except BaseException as e:
            raise BaseException("Error connecting")

    def putFile(self, path_to_local_file, file_name, path_to_remote_folder):
        if not self.sftp.exists(path_to_remote_folder):
            raise FileNotFoundError("Remote folder does not exist!")
        if not os.path.isfile(path_to_local_file + file_name):
            raise FileNotFoundError("Local file does not exist > " + path_to_local_file + file_name)
        with self.sftp.cd('/'):             # temporarily chdir to public
            print("Putting file to {0}...".format(path_to_remote_folder))
            self.sftp.chdir(path_to_remote_folder)
            self.sftp.put(path_to_local_file + file_name)  # upload file to public/ on remote

            ## Check if file exists in local folder
            if self.sftp.exists(file_name):
                print("Put file successful!")
                return True
            else:
                raise FileNotFoundError("File not found in remote directory!")
        return False

    def getFile(self, path_to_remote_file):
        print("Getting file: {0}".format(path_to_remote_file))
        try:
            with self.sftp.cd('/'):
                self.sftp.get(path_to_remote_file)
        except BaseException as e:
            raise(e)

        ## Check if file exists in local folder
        m = re.compile('^\/(?:.+\/)*(?:.+)\/(.+)$') ## Get filename
        filename = re.match(m, path_to_remote_file).group(1)

        if os.path.isfile(filename):
            print("File transfer successful!")
            return True
        else:
            print("File transfer error!")
            return False
        return False

    def pwd(self):
        return self.sftp.pwd

    def cd(self,path):
        self.sftp.cd(path)

    def chdir(self, path = '/'):
        self.sftp.chdir(path)

    def cwd(self, path=''):
        self.sftp.cwd(path)

    def isFile(self, filename):
        return self.sftp.isfile(filename)

    def isDir(self, filename):
        return self.sftp.isdir(filename)

    def exists(self, filename):
        return self.sftp.exists(filename)

    def listDir(self):
        return self.sftp.listdir()

    def execute(self, statement):
        self.sftp.execute(statement)

    def close(self):
        self.sftp.close()
