# -*- coding: utf-8 -*-
"""
Created on Tue Jul  4 11:49:02 2017

@author: james.toh
"""

class myFTP:
    
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        
        try:
            self.ftplib = __import__('ftplib', fromlist=['FTP'])
            self.os = __import__('os')
        except BaseException as e:
            raise(e)
            
    def connect(self):
        self.ftp = self.ftplib.FTP(self.host)
        
    def login(self):
        self.ftp.login(self.user, self.password)
        
    def cwd(self, path):
        self.ftp.cwd(path)
        
    def pwd(self):
        return self.ftp.pwd()
    
    def dir(self):
        return self.ftp.dir()
    
    def mkdir(self, dirname):
        self.ftp.mkd(dirname)
    
    def close(self):
        self.ftp.close()
        
    def upload(self, local_filepath, filename):

        if not local_filepath.endswith('/'):
            local_filepath += '/'

        ext = self.os.path.splitext(filename)[1]
        
        if ext in ('.txt','.htm','.html'):
            self.ftp.storlines("STOR " +  filename, open(local_filepath + filename))
        else:
            self.ftp.storbinary("STOR " +  filename, open(local_filepath + filename, 'rb'), 262144)