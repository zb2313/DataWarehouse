# 作者：韩乃超
# 日期：2021年11月29日
import os
import json
from py2neo import Graph, Node


class MedicalGraph:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.data_path = os.path.join(cur_dir, 'movies3.json')
        self.g = Graph("http://47.103.9.250:7474/", auth=("neo4j", "test"))

    '''读取文件'''

    def read_nodes(self):
        # 共7类节点
        asin = []  # 电影的asin
        movies = []  # 名称
        actors = []  # 演员
        directors = []  # 导演
        styles = []  # 类型
        versions = []  # 版本
        times = []  # 上映时间

        movie_infos = []  # 电影信息

        # 构建节点实体关系
        belongs_to = []  # 电影名称术语类型
        do_act = []  # 演电影
        do_dir = []  # 导电影
        cooperate_aandd = []  # 演员和导演合作
        cooperate_danda = []  # 导演和演员合作
        cooperate_aanda = []  # 演员和演员合作
        cooperate_dandd = []  # 导演和导演合作
        has_version = []  # 电影版本
        has_time = []  # 电影上映时间

        count = 0
        for data in open(self.data_path, encoding='utf-8'):
            movie_dict = {}
            count += 1
            print("read", count)
            data_json = json.loads(data)
            movie = data_json['Title']
            movie_dict['Title'] = movie
            movies.append(movie)
            movie_dict['IMDB_grade'] = ''
            movie_dict['ReviewPoint'] = ''
            movie_dict['reviews1'] = ''
            movie_dict['reviews2'] = ''
            movie_dict['reviews3'] = ''
            movie_dict['reviews4'] = ''
            movie_dict['reviews5'] = ''
            if 'IMDB grade' in data_json:
                movie_dict['IMDB_grade'] = data_json['IMDB grade']
            if 'ReviewPoint' in data_json:
                movie_dict['ReviewPoint'] = data_json['ReviewPoint']
            if 'reviews' in data_json:
                if len(data_json['reviews']) == 1:
                    movie_dict['reviews1'] = list(data_json['reviews'][0].values())
                    for i in range(8):
                        movie_dict['reviews1'][i] = str(movie_dict['reviews1'][i])
                if len(data_json['reviews']) == 2:
                    movie_dict['reviews1'] = list(data_json['reviews'][0].values())
                    movie_dict['reviews2'] = list(data_json['reviews'][1].values())
                    for i in range(8):
                        movie_dict['reviews1'][i] = str(movie_dict['reviews1'][i])
                        movie_dict['reviews2'][i] = str(movie_dict['reviews2'][i])
                if len(data_json['reviews']) == 3:
                    movie_dict['reviews1'] = list(data_json['reviews'][0].values())
                    movie_dict['reviews2'] = list(data_json['reviews'][1].values())
                    movie_dict['reviews3'] = list(data_json['reviews'][2].values())
                    for i in range(8):
                        movie_dict['reviews1'][i] = str(movie_dict['reviews1'][i])
                        movie_dict['reviews2'][i] = str(movie_dict['reviews2'][i])
                        movie_dict['reviews3'][i] = str(movie_dict['reviews3'][i])

                if len(data_json['reviews']) == 4:
                    movie_dict['reviews1'] = list(data_json['reviews'][0].values())
                    movie_dict['reviews2'] = list(data_json['reviews'][1].values())
                    movie_dict['reviews3'] = list(data_json['reviews'][2].values())
                    movie_dict['reviews4'] = list(data_json['reviews'][3].values())
                    for i in range(8):
                        movie_dict['reviews1'][i] = str(movie_dict['reviews1'][i])
                        movie_dict['reviews2'][i] = str(movie_dict['reviews2'][i])
                        movie_dict['reviews3'][i] = str(movie_dict['reviews3'][i])
                        movie_dict['reviews4'][i] = str(movie_dict['reviews4'][i])
                if len(data_json['reviews']) == 5:
                    # list(a.values())
                    movie_dict['reviews1'] = list(data_json['reviews'][0].values())
                    movie_dict['reviews2'] = list(data_json['reviews'][1].values())
                    movie_dict['reviews3'] = list(data_json['reviews'][2].values())
                    movie_dict['reviews4'] = list(data_json['reviews'][3].values())
                    movie_dict['reviews5'] = list(data_json['reviews'][4].values())
                    for i in range(8):
                        movie_dict['reviews1'][i] = str(movie_dict['reviews1'][i])
                        movie_dict['reviews2'][i] = str(movie_dict['reviews2'][i])
                        movie_dict['reviews3'][i] = str(movie_dict['reviews3'][i])
                        movie_dict['reviews4'][i] = str(movie_dict['reviews4'][i])
                        movie_dict['reviews5'][i] = str(movie_dict['reviews5'][i])

            # 重点
            if 'Actors' in data_json:
                for actor in data_json['Actors']:
                    actors += [actor]
                    do_act.append([movie, actor])
                tempact = data_json['Actors']
                if len(tempact) == 2:
                    act1 = tempact[0]
                    act2 = tempact[1]
                    cooperate_aanda.append([act1, act2])
                    cooperate_aanda.append([act2, act1])

            if 'Director' in data_json:
                for director in data_json['Director']:
                    directors += [director]
                    do_dir.append([movie, director])
                tempdir = data_json['Director']
                if len(tempdir) == 2:
                    dir1 = tempdir[0]
                    dir2 = tempdir[1]
                    cooperate_aanda.append([dir1, dir2])
                    cooperate_aanda.append([dir2, dir1])
                for director in data_json['Director']:
                    if 'Actors' in data_json:
                        for actor in data_json['Actors']:
                            cooperate_danda.append([director, actor])
                            cooperate_aandd.append([actor, director])

            if 'style' in data_json:
                style = data_json['style']
                styles += [data_json['style']]
                belongs_to.append([movie, style])
            if 'version' in data_json:
                for version in data_json['version']:
                    versions += [version]
                    has_version.append([movie, version])
            if 'Date First Available' in data_json:
                tim = data_json['Date First Available']
                times += [tim]
                has_time.append([movie, tim])

            movie_infos.append(movie_dict)
        all_times = []
        for tim in times:
            if tim not in all_times:
                all_times.append(tim)
        return set(movies), set(actors), set(directors), set(styles), set(versions), movie_infos, all_times, \
               belongs_to, do_act, do_dir, cooperate_aandd, cooperate_danda, cooperate_aanda, cooperate_dandd, has_version, has_time

    '''建立节点'''

    def create_node(self, label, nodes):
        count = 0
        for node_name in nodes:
            node = Node(label, name=node_name)
            self.g.create(node)
            count += 1
            print("creatnodes", count, len(nodes))
        return

    '''创建知识图谱中心疾病的节点'''

    def create_movies_nodes(self, movie_infos):
        count = 0

        for movie_dict in movie_infos:
            node = Node("Movie", name=movie_dict['Title'], IMDBgrade=movie_dict['IMDB_grade'],
                        ReviewPoint=movie_dict['ReviewPoint'], reviews1=movie_dict['reviews1'],
                        reviews2=movie_dict['reviews2'], reviews3=movie_dict['reviews3'],
                        reviews4=movie_dict['reviews4'], reviews5=movie_dict['reviews5'])

            self.g.create(node)
            count += 1
            print("creatmovies", count)
        return

    '''创建知识图谱实体节点类型schema'''

    def create_graphnodes(self):
        movies, actors, directors, styles, versions, movie_infos, times, belongs_to, do_act, do_dir, cooperate_aandd, cooperate_danda, cooperate_aanda, cooperate_dandd, has_version, has_time = self.read_nodes()
        self.create_movies_nodes(movie_infos)
        self.create_node('Actor', actors)
        self.create_node('Director', directors)
        self.create_node('Style', styles)
        self.create_node('Version', versions)
        self.create_node('Time', times)

        return

    '''创建实体关系边'''

    def create_graphrels(self):
        movies, actors, directors, styles, versions, movie_infos, times, belongs_to, do_act, do_dir, cooperate_aandd, cooperate_danda, cooperate_aanda, cooperate_dandd, has_version, has_time = self.read_nodes()
        self.create_relationship('Movie', 'Style', belongs_to, 'belongs_to', '电影的类型')
        self.create_relationship('Movie', 'Actor', do_act, 'do_act', '演员演电影')
        self.create_relationship('Movie', 'Director', do_dir, 'do_dir', '导演导电影')
        self.create_relationship('Actor', 'Director', cooperate_aandd, 'cooperate_aandd', '演员和导演合作')
        self.create_relationship('Director', 'Actor', cooperate_danda, 'cooperate_danda', '导演和演员合作')
        self.create_relationship('Actor', 'Actor', cooperate_aanda, 'cooperate_aanda', '演员和演员合作')
        self.create_relationship('Director', 'Director', cooperate_dandd, 'cooperate_dandd', '导演和导演合作')
        self.create_relationship('Movie', 'Version', has_version, 'has_version', '电影的版本')
        self.create_relationship('Movie', 'Time', has_time, 'has_time', '电影的上映时间')

    '''创建实体关联边'''

    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        count = 0
        # 去重处理
        # set_edges = []
        # for edge in edges:
        #     set_edges.append('###'.join(edge))
        all = len((edges))
        # sall=len(set_edges)
        for edge in edges:
            # edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            # match(p:Movie{name:"TEST0"}),(q:Style{name:"General"}),z=(p)-[r:belongs_to]->(q) RETURN count(z)   match(p:Movie{name:"TEST0"}),(q:Style{name:"General"}),z=(p)-[r:belongs_to]->(q)
            # set r.num=r.num+1

            #match(p:Movie{name:"TEST1"}),(q:Style{name:"General"}),z=(p)-[r:belongs_to]->(q)
            #return r.num
            verify = 'match(p:%s{name:"%s"}),(q:%s{name:"%s"}),z=(p)-[r:%s]->(q) RETURN r.num' % (start_node, p, end_node, q, rel_type)
            r_num=self.g.run(verify)
            temp_num0=r_num.data()
            if len(temp_num0)==0:
                query = 'match(p:%s),(q:%s) where p.name="%s" and q.name="%s" create (p)-[rel:%s{name:"%s",num:%d}]->(q)' % (start_node, end_node, p, q, rel_type, rel_name, 1)
            else:
                query = 'match(p:%s{name:"%s"}),(q:%s{name:"%s"}),z=(p)-[r:%s]->(q) set r.num=r.num+1' % (start_node, p, end_node, q, rel_type)
            # temp_num1=r_num.data().pop()
            # num=list(temp_num1.values())[0]
            #
            # if num==0:
            #     query = 'match(p:%s),(q:%s) where p.name="%s" and q.name="%s" create (p)-[rel:%s{name:"%s",num:%d}]->(q)' % (start_node, end_node, p, q, rel_type, rel_name, 1)
            #     #match(p:Movie{name:"TEST0"}),(q:Style{name:"General"}),z=(p)-[r:belongs_to]->(q) RETURN z.num
            # if num!=0:
            #     query='match(p:%s{name:"%s"}),(q:%s{name:"%s"}),z=(p)-[r:%s]->(q) set r.num=r.num+1' %(start_node,p,end_node,q,rel_type)
            # 这里高斯劳资了，西巴 MATCH (<node1-label-name>:<nade1-name>),(<node2-label-name>:<node2-name>)
            # CREATE
            #   (<node1-label-name>)-[<relationship-label-name>:<relationship-name>{<define-properties-list}]
            # query = 'match(p:%s),(q:%s) where p.name="%s" and q.name="%s" create (p)-[rel:%s{name:"%s",num:%d}]->(q)' % (start_node, end_node, p, q, rel_type, rel_name, 1)
            # query = "match(p:" + start_node + "),(q:" + end_node + ") where p.name='" + p + "' and q.name='" + q + "' create (p)-[rel:" + rel_type + "{name:'" + rel_name + "'}]->(q)"
            try:
                self.g.run(query)
                count += 1
                print(rel_type, count, all)
            except Exception as e:
                print(e)
        return



if __name__ == '__main__':
    handler = MedicalGraph()
    print("step1:导入图谱节点中")
    handler.create_graphnodes()
    print("step2:导入图谱边中")
    handler.create_graphrels()

