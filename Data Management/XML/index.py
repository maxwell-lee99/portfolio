from lxml import etree, objectify
import re
import os
import sys


def printf(elems):
    if (isinstance(elems, list)):
        for elem in elems:
            if isinstance(elem, str):
                print(elem)
            else:
                print(etree.tostring(elem).decode('utf-8'))
    else: # just a single element
        print(etree.tostring(elems).decode('utf-8'))



myxml=etree.Element('index')



def print_path_of_elems(myxml,elem,file_name, elem_path="",store=""):
    
    
    previous_elements=[]
    for child in elem:
        tokens=[]
        count=1
        for x in previous_elements:
            if x == child.tag:
                count+=1
        previous_elements.append(child.tag)

        if child.attrib:
    
            dic=child.attrib
            
            for i in dic.values():
                splt=re.split(r'\W+', i)
                for x in splt:
                    tokens.append(x)

        if child.text:
            splt=re.split(r'\W+', child.text)
            for x in splt:
                if x != '':
                    tokens.append(x)
        
        

            # looping through each token
        for i in tokens:
            i=i.lower()
            found=False
            
            #first, trying to find the token already in our index. if it exists, add a provenance to that value
            for children in myxml:
                for childs in children:
                    if childs.text == i:
                        found=True
                        parent=childs.find('..')
                        child1=etree.SubElement(parent,'provenance')
                        child2=etree.SubElement(child1,'where')
                        child2.text='{}/{}[{}]'.format(store,child.tag,count)
                        child3=etree.SubElement(child1,'which')
                        child3.text=file_name
            #if the token value not found, add it to the index
            if found == False:
                childs=etree.SubElement(myxml,'token')
                child1=etree.SubElement(childs,'value')
                child1.text=i
                child2=etree.SubElement(childs,'provenance')
                child3=etree.SubElement(child2,'where')
                child3.text='{}/{}[{}]'.format(store,child.tag,count)
                child4=etree.SubElement(child2,'which')
                child4.text=file_name   
                
                
                    
                
                
                
            
            
        if child.getchildren():
            # node with child elements => recurse
            print_path_of_elems(myxml,child,file_name, "%s/%s" % (elem_path, child.tag),"%s/%s[%i]" % (elem_path, child.tag,count))

directory=sys.argv[1]
files=os.listdir(directory)







for i in files:
    parser=etree.XMLParser(remove_comments=True)
    tree=objectify.parse('{}/{}'.format(directory,i),parser=parser)
    root=tree.getroot()
    print_path_of_elems(myxml,root,i,root.tag,root.tag)



doc=etree.ElementTree(myxml)
doc.write(sys.argv[2],pretty_print=True)