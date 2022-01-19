from lxml import etree
import re
import os
import sys

#check if the attributes are there

def printf(elems):
    if (isinstance(elems, list)):
        for elem in elems:
            if isinstance(elem, str):
                print(elem)
            else:
                print(etree.tostring(elem).decode('utf-8'))
    else: # just a single element
        print(etree.tostring(elems).decode('utf-8'))



# index=etree.parse('index.xml')

# searches=[]

# for i in x:
#     for y in range(len(i.xpath('provenance/which/text()'))):
#         dic={'value':i.xpath('value/text()')[0],
#             'file':i.xpath('provenance/which/text()')[y],
#             'path':i.xpath('provenance/where/text()')[y]}
        
#         searches.append(dic)
    
# clean_searches=[dict(t) for t in {tuple(d.items()) for d in searches}]


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
    



index=sys.argv[1]
directory=sys.argv[2]
files=os.listdir(directory)
search=sys.argv[3]
search=str.split(search)
search=[x.lower() for x in search]
store=[]
index=etree.parse(index)
root=index.getroot()

found=False

store=[]
for i in search:
    if index.xpath('//value[text()="{}"]'.format(i)):
        for child in root:
            for childs in child:
                if childs.text == i:
                    parent=childs.getparent()
                    for i in range(1,len(parent.getchildren())):
                        child2=parent.getchildren()[i]
                        dic={'where':child2.getchildren()[0].text,
                            'which': child2.getchildren()[1].text}
                        store.append(dic)
                    
        found=True


if found == False:
    print('No such tokens')


uniques=[]
for i in store:
    if i not in uniques:
        uniques.append(i)


for i in uniques:
    tree=etree.parse('{}/{}'.format(directory,i['which']))
    print('Element:')
    printf(tree.xpath('/{}'.format(i['where'])))
    print('File: {}'.format(i['which']))
