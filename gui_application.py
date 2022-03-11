import os, sys, json
from PyQt6.QtGui import *
from PyQt6.QtWidgets import *
from PyQt6.QtCore import *
import pandas as pd
from pathlib import Path
from main import *

class MainWindow(QMainWindow):
    def __init__(self, *args, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        self.data_filepath = "data.json" 
        self.setData()      
        self.initUI()

    def closeEvent(self, event:QCloseEvent):
        with open(self.data_filepath, 'w+') as data_file:
            json.dump(self.data, data_file)
            data_file.close()

    def setData(self):
        self.data = {}
        if not os.path.isfile(self.data_filepath):
            self.data["csv_dir"] = str(os.path.expanduser('~'))
            self.data["out_dir"] = str(os.path.expanduser('~'))
            self.data["shp_dir"] = str(os.path.expanduser('~')) 
            self.data["columns_dict"] = {"Master": "", "MasterURL": "", "Slave": "", "SlaveURL": ""}
            self.data["settings"] = {"keep_downloads": False, "subset": False}
        else:
            with open(self.data_filepath, 'r') as data_file:
                self.data = json.load(data_file)
                data_file.close() 

    def setPathGrid(self):
        self.pathLabel = QLabel("CSV File")
        self.pathLineEdit = QLineEdit()
        self.pathButton = QPushButton("Find")
        self.pathButton.clicked.connect(self.getCSVFile)
        self.pathError = QLabel("")
        self.pathError.setStyleSheet("color: red")
        self.pathError.hide()
        
        self.pathGrid = QGridLayout()
        self.pathGrid.addWidget(self.pathLabel, 0, 0, 1, 6)
        self.pathGrid.addWidget(self.pathLineEdit, 1, 0, 1, 5)
        self.pathGrid.addWidget(self.pathButton, 1, 5, 1, 1)
        self.pathGrid.addWidget(self.pathError, 2, 0, 1, 6)

    def setColGrid(self):
        self.masterLabel = QLabel("Master Column")
        self.masterComboBox = QComboBox()
        self.masterUrlLabel = QLabel("MasterURL Column")
        self.masterUrlComboBox = QComboBox()
        self.slaveLabel = QLabel("Slave Column")
        self.slaveComboBox = QComboBox()
        self.slaveUrlLabel = QLabel("SlaveURL Column")
        self.slaveUrlComboBox = QComboBox()
        self.loadProductsButton = QPushButton("Load Products")
        self.loadProductsButton.clicked.connect(self.populateProductsTable)

        self.colGrid = QGridLayout()
        self.colGrid.addWidget(self.masterLabel, 0, 0, 1, 2)
        self.colGrid.addWidget(self.masterComboBox, 0, 2, 1, 3)
        self.colGrid.addWidget(self.masterUrlLabel, 1, 0, 1, 2)
        self.colGrid.addWidget(self.masterUrlComboBox, 1, 2, 1, 3)
        self.colGrid.addWidget(self.slaveLabel, 2, 0, 1, 2)
        self.colGrid.addWidget(self.slaveComboBox, 2, 2, 1, 3)
        self.colGrid.addWidget(self.slaveUrlLabel, 3, 0, 1, 2)
        self.colGrid.addWidget(self.slaveUrlComboBox, 3, 2, 1, 3)
        self.colGrid.addWidget(self.loadProductsButton, 4, 4, 1, 1)

    def setShpGrid(self):
        self.shpLabel = QLabel("Shapefile")
        self.shpLineEdit = QLineEdit()
        self.shpButton = QPushButton("Find")
        self.shpButton.clicked.connect(self.getShapeFile)

        self.shpGrid = QGridLayout()
        self.shpGrid.addWidget(self.shpLabel, 0, 0, 1, 6)
        self.shpGrid.addWidget(self.shpLineEdit, 1, 0, 1, 5)
        self.shpGrid.addWidget(self.shpButton, 1, 5, 1, 1)

    def setOutputPathGrid(self):
        self.outputPathLabel = QLabel("Output")
        self.outputPathLineEdit = QLineEdit()
        self.outputPathButton = QPushButton("Browse")
        self.outputPathButton.clicked.connect(self.getOutputPath)

        self.outputPathGrid = QGridLayout()
        self.outputPathGrid.addWidget(self.outputPathLabel, 0, 0, 1, 6)
        self.outputPathGrid.addWidget(self.outputPathLineEdit, 1, 0, 1, 5)
        self.outputPathGrid.addWidget(self.outputPathButton, 1, 5, 1, 1)

    def setEarthDataGrid(self):
        self.earthDataLabel = QLabel("Earthdata Credintials")
        self.usernameLabel = QLabel("Username")
        self.usernameLineEdit = QLineEdit()
        self.passwordLabel = QLabel("Password")
        self.passwordLineEdit = QLineEdit()
        self.passwordLineEdit.setEchoMode(QLineEdit.EchoMode.Password)

        self.earthDataGrid = QGridLayout()
        self.earthDataGrid.addWidget(self.earthDataLabel, 0, 0, 1, 6)
        self.earthDataGrid.addWidget(self.usernameLabel, 1, 0, 1, 3)
        self.earthDataGrid.addWidget(self.passwordLabel, 1, 3, 1, 3)
        self.earthDataGrid.addWidget(self.usernameLineEdit, 2, 0, 1, 3)
        self.earthDataGrid.addWidget(self.passwordLineEdit, 2, 3, 1, 3)

    def initUI(self):
        self.setWindowTitle('Application')
        self.setMinimumSize(QSize(800, 450))
        self.resize(QSize(1600, 900))

        self.setPathGrid()
        self.setColGrid()
        self.setShpGrid()
        self.setOutputPathGrid()
        self.setEarthDataGrid()

        self.csvVbox = QVBoxLayout()
        self.csvVbox.addLayout(self.pathGrid)
        self.csvVbox.addLayout(self.colGrid)
        self.csvVbox.addLayout(self.shpGrid)
        self.csvVbox.addLayout(self.outputPathGrid)
        self.csvVbox.addLayout(self.earthDataGrid)
        self.csvVbox.addStretch()

        self.productSpecLabel = QLabel("Settings")
        self.keepDownloadCheckBox = QCheckBox("  Keep downloaded image after processing.")
        self.keepDownloadCheckBox.setChecked(self.data["settings"].get("keep_downloads", False))
        self.keepDownloadCheckBox.toggled.connect(self.updateSettings)
        self.subsetCheckBox = QCheckBox("  Subset images according to study area.")
        self.subsetCheckBox.setChecked(self.data["settings"].get("subset", False))
        self.subsetCheckBox.toggled.connect(self.updateSettings)
        self.processButton = QPushButton("Start Process")
        self.processButton.clicked.connect(self.getOrthorectifiedProduct)

        self.productSpecVbox = QVBoxLayout()
        self.productSpecVbox.addWidget(self.productSpecLabel)
        self.productSpecVbox.addWidget(self.keepDownloadCheckBox)
        self.productSpecVbox.addWidget(self.subsetCheckBox)
        self.productSpecVbox.addWidget(self.processButton)
        self.productSpecVbox.addStretch()        

        self.productsTable = QTableWidget()
        self.productsTable.setColumnCount(4)
        self.productsTable.setHorizontalHeaderLabels(["Master", "MasterURL", "Slave", "SlaveURL"])

        self.step = 0
        self.processLabel = QLabel("Downloading product")
        self.pbar = QProgressBar(self)
        self.pbar.setFormat("   %v of %m")


        self.grid = QGridLayout()
        self.grid.addLayout(self.csvVbox, 0, 0, 1, 12)
        self.grid.addLayout(self.productSpecVbox, 0, 13, 1, 12)
        self.grid.addWidget(self.productsTable, 1, 0, 1, 38)
        self.grid.addWidget(self.processLabel, 2, 0, 1, 19)
        self.grid.addWidget(self.pbar, 2, 19, 1, 19)

        self.centralWidget = QWidget()
        self.centralWidget.setLayout(self.grid)

        self.setCentralWidget(self.centralWidget) 
        self.show()

    def updateSettings(self):
        self.data["settings"] = {"keep_downloads": self.keepDownloadCheckBox.isChecked(), "subset": self.subsetCheckBox.isChecked()}

    def addProgress(self):
        self.step = self.step + 1
        self.pbar.setValue(self.step)

    def getCSVFile(self):
        self.pathError.hide()
        file = QFileDialog.getOpenFileName(self, 'Open file', self.data["csv_dir"], "CSV File (*.csv)")
        if file[0]:
            self.data["csv_dir"] = os.path.dirname(file[0])    
            self.pathLineEdit.setText(file[0])
            try:
                self.df = pd.read_csv(file[0], header=0, dtype=object)
            except:
                self.pathError.setText("Format not supported.")
                self.pathError.show()
            else:
                self.masterComboBox.clear()
                self.masterComboBox.addItems(self.df.columns)
                self.masterUrlComboBox.clear()
                self.masterUrlComboBox.addItems(self.df.columns)
                self.slaveComboBox.clear()
                self.slaveComboBox.addItems(self.df.columns)
                self.slaveUrlComboBox.clear()
                self.slaveUrlComboBox.addItems(self.df.columns)
                if self.data["columns_dict"]["Master"] in self.df.columns:
                    self.masterComboBox.setCurrentText(self.data["columns_dict"]["Master"])
                if self.data["columns_dict"]["MasterURL"] in self.df.columns:
                    self.masterUrlComboBox.setCurrentText(self.data["columns_dict"]["MasterURL"])
                if self.data["columns_dict"]["Slave"] in self.df.columns:
                    self.slaveComboBox.setCurrentText(self.data["columns_dict"]["Slave"])
                if self.data["columns_dict"]["SlaveURL"] in self.df.columns:
                    self.slaveUrlComboBox.setCurrentText(self.data["columns_dict"]["SlaveURL"])

    def populateProductsTable(self):
        if self.pathLineEdit.text() == "":
            self.pathError.setText("Please choose a CSV file to be loaded.")
            self.pathError.show()
            return
        self.column_dict = {"Master": str(self.masterComboBox.currentText()), "MasterURL": str(self.masterUrlComboBox.currentText()),
                            "Slave": str(self.slaveComboBox.currentText()), "SlaveURL": str(self.slaveUrlComboBox.currentText())}
        self.data["columns_dict"] = self.column_dict
        self.products = []
        self.productUrls = []
        self.productsTable.setRowCount(self.df.shape[0])
        for ix in range(0, self.df.shape[0]):
            self.productsTable.setItem(ix, 0, QTableWidgetItem(str(self.df[self.column_dict["Master"]][ix])))
            self.productsTable.setItem(ix, 1, QTableWidgetItem(str(self.df[self.column_dict["MasterURL"]][ix])))
            self.productsTable.setItem(ix, 2, QTableWidgetItem(str(self.df[self.column_dict["Slave"]][ix])))
            self.productsTable.setItem(ix, 3, QTableWidgetItem(str(self.df[self.column_dict["SlaveURL"]][ix])))
        self.productsTable.resizeColumnsToContents()

    def getOutputPath(self):
        directory = str(QFileDialog.getExistingDirectory(self, "Select Directory", self.data["out_dir"]))
        if directory:
            self.data["out_dir"] = directory
            self.outputPathLineEdit.setText(directory)
            self.outputFolder = directory

    def getShapeFile(self):
        file = QFileDialog.getOpenFileName(self, 'Open shapefile', self.data["shp_dir"], "SHP File (*.shp)")
        if file[0]:
            self.data["shp_dir"] = file[0]
            self.shpLineEdit.setText(file[0])
            self.shapefile = readShapefile(file[0])

    def getOrthorectifiedProduct(self):
        self.username = self.usernameLineEdit.text()
        self.password = self.passwordLineEdit.text()
        for i, entry  in self.df.iterrows():
            masterDownloadPath = downloadFile(entry[self.column_dict["MasterURL"]], self.outputFolder, self.username, self.password)
            slaveDownloadPath = downloadFile(entry[self.column_dict["SlaveURL"]],  self.outputFolder, self.username, self.password)
            
            infSplits = [] 
            for IW in ["IW1", "IW2", "IW3"]:
                # debInf = getDeburstedInterferogram(self.outputFolder, masterDownloadPath, slaveDownloadPath, IW, self.shapefile)
                # if debInf is not None:
                #     deburstedInterferogramPaths.append(debInf)
                    
                masterTopsarSplitPath = topsarSplitProduct(self.outputFolder, masterDownloadPath, IW, self.shapefile)
                slaveTopsarSplitPath = topsarSplitProduct(self.outputFolder, slaveDownloadPath, IW, self.shapefile)
                if masterTopsarSplitPath is not None and slaveTopsarSplitPath is not None:
                    masterApplyOrbitPath = applyOrbit(self.outputFolder, masterTopsarSplitPath)
                    slaveApplyOrbitPath = applyOrbit(self.outputFolder, slaveTopsarSplitPath)
                    coregisteredPath = coregisterProducts(self.outputFolder, masterApplyOrbitPath, slaveApplyOrbitPath, IW)
                    interferogramPath = createInterferogram(self.outputFolder, coregisteredPath)
                    infSplits.append(deburst(self.outputFolder, interferogramPath))

            mergePath = merge(self.outputFolder, infSplits)
            subsetPath = subset(self.outputFolder, mergePath, self.shapefile)
            removePhasePath = removePhase(self.outputFolder, subsetPath)
            multilookPath = multilook(self.outputFolder, removePhasePath)
            filterPath = filter(self.outputFolder, multilookPath)





            


def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    app.exec()


if __name__ == '__main__':
    main()