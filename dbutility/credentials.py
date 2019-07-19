import matplotlib
#matplotlib.use("TkAgg")
from matplotlib import pyplot as plt
import tkinter as tk

class Credentials(object):
    #CGP 13/07/2017
    #Version 1.0
	#
    # This class creates a credentials object that holds the username
    # password of anyone attempting to connect to the MATLAB server
    # class will determine the desired filePath to return data to based
    # on the username.
 
    def __init__(self, path=None): 
        self.username = ''
        self.password = ''
        if path == None:
            self.filePath = ''
        else:
            self.filePath = path
        
        
    def set_path(self, path):
        self.filePath = path
        
    def capture_credentials(self):
        
        def fetch(self, entries):
            self.password = str(entries[1][1].get())
            #for entry in entries:
                #field = entry[0]
                #text  = entry[1].get()
                #print('%s: "%s"' % (field, text))
                
            root.destroy()
            return entries

        def on_username_entry(self,entries):
                self.username = str(entries[0][1].get())
                self.password = str(entries[1][1].get())
                print(self.password)
                #self.set_path()
                entries[2][1].delete(0,1000)
                entries[2][1].insert(0, self.filePath) 
                return self.entries

        def makeform(self, root, fields):
            self.entries = []
            for idx,field in enumerate(fields):
                row = tk.Frame(root)
                lab = tk.Label(row, width=15, text=field, anchor='w')
                if idx == 1:
                    ent = tk.Entry(row, show = '*')
                else:
                    ent = tk.Entry(row, show = '')
                    
                row.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)
                lab.pack(side=tk.LEFT)
                ent.pack(side=tk.RIGHT, expand=tk.YES, fill=tk.X)
                self.entries.append((field, ent))
            return self.entries

        root = tk.Tk()
        fields = 'Username', 'Password', 'File path'
        ents = makeform(self, root, fields)

        root.bind('<Return>', (lambda event, e=ents: fetch(self,e)))
        root.bind('<Tab>', (lambda event, e=ents: on_username_entry(self,e)))
        #root.bind('<Button-1>', (lambda event, e=ents: on_username_entry(self,e)))
        b1 = tk.Button(root, text='OK', command=(lambda e=ents: fetch(self, e)))
        b1.pack(side=tk.LEFT, padx=5, pady=5)
        root.mainloop()
        #self.set_path()