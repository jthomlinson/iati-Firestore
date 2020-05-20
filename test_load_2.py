from google.cloud import firestore
import sys
import XMLFileConcat as xc
import defusedxml.cElementTree as ET
import xmlschema
from xmljson import badgerfish as bf #pip install
import regex as re #pip install
import xmlschema as xs #pip install
from xmlschema.validators import XsdElement
from collections import OrderedDict

from multiprocessing.pool import ThreadPool
from datetime import datetime

class IatiFirestoreThreader(object):

    def __init__(self, db, parentKey, parentValue, treeXSD, firestorePath, XsdPath):
        self.db = db
        self.parentKey = parentKey
        self.parentValue = parentValue
        self.treeXSD = treeXSD
        self.firestorePath = firestorePath
        self.XsdPath = XsdPath
        
    def threadList(self):
        with ThreadPool(200) as p:
            p.map(self.startWriteThread, self.parentValue)


    def startWriteThread(self, listElement):
        print(type(listElement))
        element = self.treeXSD.find(self.XsdPath)
        if isTerminalElement(element):
            print('Store 1 - Path: ', self.firestorePath, ' XSD path:', self.XsdPath, ' Element: ', type(listElement))
            doc_ref = self.db.collection(self.firestorePath).document()
            doc_ref.set(listElement)
        else:
            childDirect = {}
            try:
                for childKey, childValue in listElement.items():
                    if type(childValue) is OrderedDict or type(childValue) is list:
                        print(f'Contains {childKey}', '  ', type(childValue))
                        childDirect[childKey] = childValue
            except Exception as err:
                print('Exception: expected OrderedDirect (1): ', err)
                exit(0)
    #           Write listElement to Firestore
            for childKey in childDirect:
                del(listElement[childKey])
            subcollectionPath = self.firestorePath
            print('Store 2 - Path: ', subcollectionPath, ' XSD path:', self.XsdPath, ' Element: ', listElement)
            doc_ref = self.db.collection(subcollectionPath).document()
            doc_ref.set(listElement)
#           Call pruneTree for each item in childDirect
            for childKey, childValue in childDirect.items():
                pruneTree_0(self.db, childKey, childValue, self.treeXSD, subcollectionPath + '/' + doc_ref.id + '/' + childKey, self.XsdPath + '/' + childKey)

    
        


def main(argv):
#    deleteDocs()
#    help(db)
    loadDB()
#    readDB()
#    readXSD('/Users/john/Development/HumAI_data/Schema/iati-activities-schema.xsd')

def xpathToName(root, name):
    tag = None
    for x in root.findall('.[@name=\"'+name+'\"]'):
        tag = x.tag
        break
    if tag == None:
        for child in root:
            childTag = xpathToName(child, name)
            if childTag != None:
                tag = root.tag + '/' + childTag
    return(tag)

def isTerminalElement(element):
    limited = True
    if element == None:
        pass
    elif element.occurs[1] == None or element.occurs[1] > 1:
        limited = False
    else:
        for n in element:
            if type(n) is XsdElement:
                if not isTerminalElement(n):
                    limited = False
                    break
    return(limited)
            

def depthXSD(element):
    thisCount = 0
    unlimited = 0
    for n in x:
        if type(n) is XsdElement:
            returnCount = depth(n)
            if  thisCount < returnCount:
                thisCount = returnCount
    if x.occurs[1] == None or x.occurs[1] > 1:
        unlimited = 1
    return(thisCount + unlimited)
        
def readXSD(fileName):
    try:
        xsdTree = ET.parse(fileName)
    except Exception as err:
        print('Exception parsing XSD: ', err)
        exit(0)
    return(xsdTree)
#    print(xpathToName(root, 'iati-activities'))
#    return
#    print(root.attrib)
#    for x in root.findall("./[@version='2.03']"):
#        print(x.tag)
#    for x in root.iter():
#        print(x)





def pruneTree_0(db, parentKey, parentValue, treeXSD, firestorePath = '', XsdPath = ''):
    print('Prune call - Path: ', firestorePath, ' XSD path:', XsdPath, ' Parent Key: ', parentKey, ' Parent Value: ', type(parentValue))
    if parentKey == None:
        parentKey = list(parentValue.keys())[0]
        parentValue = parentValue[parentKey]
        firestorePath = u'' + parentKey
        XsdPath = u'/' + parentKey
        
    print(f'Parent key: {parentKey}')
    
    if type(parentValue) is list:
#       We are creating a collection for the items in the list
#       For each list item that is a directory, put its elements into a directory and delete from the current
        for listElement in parentValue:
#            All the elements should be OrderedDict
#            elementDepth = depth(listElement)
            element = treeXSD.find(XsdPath)
            if isTerminalElement(element):
                print('Store_0 1 - Path: ', firestorePath, ' XSD path:', XsdPath, ' Element: ', parentValue)
                doc_ref = db.collection(firestorePath).document()
                doc_ref.set(listElement)
            else:
                childDirect = {}
                try:
                    for childKey, childValue in listElement.items():
                        if type(childValue) is OrderedDict or type(childValue) is list:
                            print(f'Contains {childKey}', '  ', type(childValue))
                            childDirect[childKey] = childValue
                except Exception as err:
                    print('Exception: expected OrderedDirect (1): ', err)
                    exit(0)
        #           Write listElement to Firestore
                for childKey in childDirect:
                    del(listElement[childKey])
                subcollectionPath = firestorePath
                print('Store_0 2 - Path: ', subcollectionPath, ' XSD path:', XsdPath, ' Element: ', listElement)
                doc_ref = db.collection(subcollectionPath).document()
                doc_ref.set(listElement)
#           Call pruneTree for each item in childDirect
                for childKey, childValue in childDirect.items():
                    print('Store_0 2 - key: ' + childKey + '  child value type:', type(childValue))
                    if type(childValue) is list:
                        threader = IatiFirestoreThreader(db, childKey, childValue, treeXSD, subcollectionPath + '/' + doc_ref.id + '/' + childKey, XsdPath + '/' + childKey)
                        threader.threadList()
                    else:
                        pruneTree_0(db, childKey, childValue, treeXSD, subcollectionPath + '/' + doc_ref.id + '/' + childKey, XsdPath + '/' + childKey)

    elif type(parentValue) is OrderedDict:
#       We have a directory of elements, each of which needs to become a subcollection
#       For each item in the directory, put its elements into a directory and delete from the current
#       Write the item as a document to a collection with the name of the parentKey
#       For each item in the child directory, call prune tree
        element = treeXSD.find(XsdPath)
        if isTerminalElement(element):
            print('Store_0 3 - Path: ', firestorePath, ' XSD path:', XsdPath, ' Element: ', parentValue)
            doc_ref = db.collection(firestorePath).document()
            doc_ref.set(parentValue)
        else:
            childDirect = {}
            try:
                for childKey, childValue in parentValue.items():
                    print('Child :', childKey, type(childValue))
                    if type(childValue) is OrderedDict or type(childValue) is list:
                        print(f'Contains {childKey}', '  ', type(childValue))
                        childDirect[childKey] = childValue
            except Exception as err:
                print('Exception: expected OrderedDirect (2): ', err)
                exit(0)
            for childKey in childDirect:
                del(parentValue[childKey])

    #           Write listElement to Firestore
            subcollectionPath = firestorePath
            
            print('Store_0 4 - Path: ', subcollectionPath, ' XSD path:', XsdPath, ' Element: ', parentValue)
            doc_ref = db.collection(subcollectionPath).document()
            doc_ref.set(parentValue)
    #           Call pruneTree for each item in childDirect

            for childKey, childValue in childDirect.items():
                print('Store_0 4 - key: ' + childKey + '  child value type:', type(childValue))
                if type(childValue) is list:
                    threader = IatiFirestoreThreader(db, childKey, childValue, treeXSD, subcollectionPath + '/' + doc_ref.id + '/' + childKey, XsdPath + '/' + childKey)
                    threader.threadList()
                else:
                    pruneTree_0(db, childKey, childValue, treeXSD, subcollectionPath + '/' + doc_ref.id + '/' + childKey, XsdPath + '/' + childKey)
    else:
        print("Type not matched")
        exit(0)


def pruneTree(db, parentKey, parentValue, treeXSD, firestorePath = '', XsdPath = ''):
    print('Prune call - Path: ', firestorePath, ' XSD path:', XsdPath, ' Parent Key: ', parentKey, ' Parent Value: ', type(parentValue))
    if parentKey == None:
        parentKey = list(parentValue.keys())[0]
        parentValue = parentValue[parentKey]
        firestorePath = u'' + parentKey
        XsdPath = u'/' + parentKey
        
    print(f'Parent key: {parentKey}')
    
    if type(parentValue) is list:
#       We are creating a collection for the items in the list
#       For each list item that is a directory, put its elements into a directory and delete from the current
        for listElement in parentValue:
#            All the elements should be OrderedDict
#            elementDepth = depth(listElement)
            element = treeXSD.find(XsdPath)
            if isTerminalElement(element):
                print('Store 1 - Path: ', firestorePath, ' XSD path:', XsdPath, ' Element: ', parentValue)
                doc_ref = db.collection(firestorePath).document()
                doc_ref.set(listElement)
            else:
                childDirect = {}
                try:
                    for childKey, childValue in listElement.items():
                        if type(childValue) is OrderedDict or type(childValue) is list:
                            print(f'Contains {childKey}', '  ', type(childValue))
                            childDirect[childKey] = childValue
                except Exception as err:
                    print('Exception: expected OrderedDirect (1): ', err)
                    exit(0)
        #           Write listElement to Firestore
                for childKey in childDirect:
                    del(listElement[childKey])
                subcollectionPath = firestorePath
                print('Store 2 - Path: ', subcollectionPath, ' XSD path:', XsdPath, ' Element: ', listElement)
                doc_ref = db.collection(subcollectionPath).document()
                doc_ref.set(listElement)
#           Call pruneTree for each item in childDirect
                for childKey, childValue in childDirect.items():
                    pruneTree(db, childKey, childValue, treeXSD, subcollectionPath + '/' + doc_ref.id + '/' + childKey, XsdPath + '/' + childKey)

    elif type(parentValue) is OrderedDict:
#       We have a directory of elements, each of which needs to become a subcollection
#       For each item in the directory, put its elements into a directory and delete from the current
#       Write the item as a document to a collection with the name of the parentKey
#       For each item in the child directory, call prune tree
        element = treeXSD.find(XsdPath)
        if isTerminalElement(element):
            print('Store 3 - Path: ', firestorePath, ' XSD path:', XsdPath, ' Element: ', parentValue)
            doc_ref = db.collection(firestorePath).document()
            doc_ref.set(parentValue)
        else:
            childDirect = {}
            try:
                for childKey, childValue in parentValue.items():
                    print('Child :', childKey, type(childValue))
                    if type(childValue) is OrderedDict or type(childValue) is list:
                        print(f'Contains {childKey}', '  ', type(childValue))
                        childDirect[childKey] = childValue
            except Exception as err:
                print('Exception: expected OrderedDirect (2): ', err)
                exit(0)
            for childKey in childDirect:
                del(parentValue[childKey])

    #           Write listElement to Firestore
            subcollectionPath = firestorePath
            
            print('Store 4 - Path: ', subcollectionPath, ' XSD path:', XsdPath, ' Element: ', parentValue)
            doc_ref = db.collection(subcollectionPath).document()
            doc_ref.set(parentValue)
    #           Call pruneTree for each item in childDirect
            for childKey, childValue in childDirect.items():
                pruneTree(db, childKey, childValue, treeXSD, subcollectionPath + '/' + doc_ref.id + '/' + childKey, XsdPath + '/' + childKey)
    else:
        print("Type not matched")
        exit(0)


def loadDB():
    actXSD = xmlschema.XMLSchema('/Users/john/Development/HumAI_data/Schema/iati-activities-schema.xsd')
    orgXSD = xmlschema.XMLSchema('/Users/john/Development/HumAI_data/Schema/iati-organisations-schema.xsd')
    path = xc.argToPath("/Users/john/Development/HumAI_data/test_data/ManualTest_3")
    
    db = firestore.Client()

    fConcat = xc.XMLFileConcat(path, None, None, None)
    print(datetime.now())
    for tree in fConcat.genTree():
        tree_ = bf.data(tree.getroot())
        if "iati-activities" in tree_:
            pruneTree_0(db, None, tree_, actXSD)
        elif "iati-organisations" in tree_:
            pruneTree_0(db, None, tree_, orgXSD)
        else:
            pass
        break
    print(datetime.now())


    
def readDB():
    db = firestore.Client()

    users_ref = db.collection(u'iati-activities')
    docs = users_ref.stream()

    for doc in docs:
        print(u'{}'.format(doc.id))

def deleteDocs():
    db = firestore.Client()

    users_ref = db.collection(u'iati-activities')
    docs = users_ref.stream()
    ct = 0
    for doc in docs:
        users_ref.document(doc.id).delete()
        ct += 1
    print('Deleted {}'.format(ct))
        


if (__name__ == '__main__'):
    main(sys.argv[1:])
