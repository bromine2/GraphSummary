package graphSummary;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.*;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;


class TestRelationshipType implements   RelationshipType {
	public String type = "" ;

	public TestRelationshipType(String type) {
		this.type = type;
	}

	@Override
	public String name() {
		return type;
	}
}

class NodePair{
	long u, v;
	Double scost;
	NodePair(long u, long v){
		if(u < v){
			this.u = u;
			this.v = v;
		}
		else{
			this.u = v;
			this.v = u;
		}
		this.scost = 0.0;
	}

	NodePair(long u, long v, double scost){
		if(u < v){
			this.u = u;
			this.v = v;
		}
		else{
			this.u = v;
			this.v = u;
		}
		this.scost = scost;
	}

	void setSCost(double scost){
		this.scost =  scost;
	}

	double getSCost(){
		return this.scost;
	}

	@Override
	public String toString(){
		return "("+u+","+v+") - "+scost;
	}
}

class PairSCost{
	Map<Long, HashMap<Long, Double>> map;

	PairSCost(){
		map = new HashMap<Long, HashMap<Long, Double>>();
	}

	void put(Long u, Long v, Double cost){

		if(u > v){
			Long temp = u;
			u = v;
			v = temp;
		}

		if(!map.containsKey(u)){
			map.put(u, new HashMap<Long, Double>());
		}
		map.get(u).put(v, cost);

	}

	Double get(Long u, Long v){
		if(u > v){
			Long temp = u;
			u = v;
			v =  temp;
		}
		if(map.containsKey(u)){
			Map<Long, Double> t = map.get(u);
			if(t.containsKey(v)){
				return t.get(v);
			}
		}
		return -1.0;
	}

	void set(Long u, Long v, double scost){
		if(u > v){
			Long temp = u;
			u = v;
			v =  temp;
		}
		if(map.containsKey(u)){
			Map<Long, Double> t = map.get(u);
			if(t.containsKey(v)){
				t.put(v, scost);
			}
		}
	}

	void delete(Long u, Long v){
		if(u > v){
			Long temp = u;
			u = v;
			v = temp;
		}
		if(map.containsKey(u)){
			Map<Long, Double> t = map.get(u);
			if(t.containsKey(v)){
				t.clear();
			}
		}
	}

	boolean isAvailable(Long u, Long v){
		if(u > v){
			Long temp = u;
			u = v;
			v =  temp;
		}
		if(map.containsKey(u)){
			Map<Long, Double> t = map.get(u);
			if(t.containsKey(v)){
				return true;
			}
		}
		return false;
	}

	void print(){
		for (Map.Entry<Long, HashMap<Long, Double>> u : map.entrySet()) {
			for(Map.Entry<Long, Double> v : u.getValue().entrySet()){
				System.out.println(u.getKey()+","+v.getKey()+" - "+v.getValue());
			}
		}
	}


}

public class GreedySummary {

	GraphDatabaseService G; //图数据
	PriorityQueue<NodePair> maxHeap; //实现队列，总是获取优先级最高的元素
	PairSCost scost;
	Set<Long> Vg, Vs;//顶点集合
	Map<Long, HashSet<Long>> Eg, Es;//边集合
	Map<Long, HashSet<Long>> superNodeElements; //超点元素
	Map<Long, HashSet<Long>> ES, pC, nC;

	GreedySummary(GraphDatabaseService graph)
	{
		this.Vg = new HashSet<Long>(); //初始化工作
		this.Vs = new HashSet<Long>();
		this.Eg = new HashMap<Long, HashSet<Long>>();
		this.Es = new HashMap<Long, HashSet<Long>>();
		this.superNodeElements = new HashMap<Long, HashSet<Long>>();
		this.scost = new PairSCost();
		this.maxHeap = new PriorityQueue<NodePair>(new Comparator<NodePair>() {
			@Override
			public int compare(NodePair arg0, NodePair arg1) {
				// TODO Auto-generated method stub
				return arg1.scost.compareTo(arg0.scost);
			}
		});
		//ExecutionResult exec = (ExecutionResult) G.execute("match (n)-[r]->(m) return count(r)");
		this.G = graph;
		//initializing nodes and edges
		for(Node n: G.getAllNodes()){//获取节点、边的id信息
			Vg.add(n.getId());
			Vs.add(n.getId());
			Eg.put(n.getId(), new HashSet<Long>());
			Es.put(n.getId(), new HashSet<Long>());
			superNodeElements.put(n.getId(), new HashSet<Long>());
			superNodeElements.get(n.getId()).add(n.getId());
			for(Relationship r: n.getRelationships()){
				Eg.get(n.getId()).add(r.getOtherNode(n).getId());
				Es.get(n.getId()).add(r.getOtherNode(n).getId());
			}
		}

		this.pC = new HashMap<Long, HashSet<Long>>();
		this.nC = new HashMap<Long, HashSet<Long>>();
		this.ES = new HashMap<Long, HashSet<Long>>();
	}

	//计算损失
	double calc_cost(long u, Map<Long, HashSet<Long>> Et, HashSet<Long> Vt) throws Exception
	{
		double cost = 0.0;
		double pi, actual_edges;
		//for(Long x : Et.get(u)){
		//if(superNodeElements.get(u).contains(1L) && superNodeElements.get(u).contains(2L))
		// System.out.println("neighbors of "+u+" "+get_neighbors(u, Vt));
		for(Long x: get_neighbors(u, Vt)){
			if(u == x){
				int n = superNodeElements.get(u).size();
				pi = ((n*(n-1))/2) + n;
				//pi = n*n;
				actual_edges = 0;
				List<Long> temp = new ArrayList<Long>(superNodeElements.get(u));
				for(int i=0; i<temp.size()-1; i++){
					for(int j=i+1; j<temp.size(); j++){
						//if(superNodeElements.get(u).contains(1L) && superNodeElements.get(u).contains(2L))
						// System.out.println("su "+temp.get(i)+" sx"+temp.get(j));
						if(Eg.get(temp.get(i)).contains(temp.get(j)))
							actual_edges++;
					}
				}
			}
			else{
				pi = superNodeElements.get(u).size() * superNodeElements.get(x).size();
				actual_edges = 0;
				for(Long su: superNodeElements.get(u)){
					for(Long sx: superNodeElements.get(x)){
						//if(superNodeElements.get(u).contains(1L) && superNodeElements.get(u).contains(2L))
						// System.out.println("su "+su+" sx"+sx);
						if(Eg.get(su).contains(sx))
							actual_edges++;
					}
				}
			}
			cost += Math.min(pi - actual_edges + 1, actual_edges);
			//if(superNodeElements.get(u).contains(1L) && superNodeElements.get(u).contains(2L))
			// System.out.println("x"+x+" "+cost);
		}
		//if(Et.get(u).contains(u))    // self -edge
		// cost++;
		return cost;
	}

	double calc_scost(long u, long v, Map<Long, HashSet<Long>> Et, HashSet<Long> Vt) throws Exception
	{
		double cu, cv, cw ;
		cu = calc_cost(u, Et, Vt);
		cv = calc_cost(v, Et, Vt);

		//Node w = G.createNode();
		long w = -1l;  // dummy node

		superNodeElements.put(w, new HashSet<Long>());
		superNodeElements.get(w).addAll(superNodeElements.get(u));
		superNodeElements.get(w).addAll(superNodeElements.get(v));

		//update super nodes
		Vt.add(w);
		Vt.remove(u);
		Vt.remove(v);
		//update super edges

		Et.put(w, new HashSet<Long>());
		HashSet<Long> neigh = new HashSet<Long>();
		//System.out.println("u"+u+" "+Et.get(u)+" v"+v+" "+Et.get(v));
		if(Et.containsKey(u) && !Et.get(u).isEmpty())
			neigh.addAll(Et.get(u));
		if(Et.containsKey(v) && !Et.get(v).isEmpty())
			neigh.addAll(Et.get(v));
		neigh.remove(u); neigh.remove(v);
		for( Long n : neigh)
		{
			double actual_edges = 0.0;
			double pi = superNodeElements.get(w).size() * superNodeElements.get(n).size();
			for(Long su: superNodeElements.get(w)){
				for(Long sv: superNodeElements.get(n)){
					//System.out.println("su "+su+" sx"+sx);
					if(Eg.get(su).contains(sv))
						actual_edges++;
				}
			}
			if(actual_edges > ((pi + 1)/2)){
				Et.get(w).add(n);
			}
		}
		//System.out.println(w+" edges "+Et.get(w));
		if(Et.containsKey(w)){
			for(Long nw: Et.get(w)){
				Et.get(nw).add(w);
			}
		}
		if(Et.containsKey(u)){
			for(Long nu: Et.get(u)){
				if(nu != u)       // avoid removing self edge
					Et.get(nu).remove(u);
			}
		}
		if(Et.containsKey(v)){
			for(Long nv: Et.get(v)){
				if(nv != v)       // avoid removing self edge
					Et.get(nv).remove(v);
			}
		}


      /*
      Et.put(w, new HashSet<Long>());
      Et.get(w).addAll(Et.get(u));
      Et.get(w).addAll(Et.get(v)); //doubt -  add all or retain all?
      Et.get(w).remove(u);      // remove edge between u and v + self edge of u or v
      Et.get(w).remove(v);
      for(Long nw: Et.get(w))
         Et.get(nw).add(w);
      for(Long nu: Et.get(u))
         if(nu != u)                   // to avoid removing self edge
            Et.get(nu).remove(u);
      //System.out.println(Et);
      //System.out.println(u+" "+v);
      for(Long nv: Et.get(v))
         if(nv != v)                   // to avoid removing self edge
            Et.get(nv).remove(v);
      */
		//self edge - if edge between u and v


		if((Et.containsKey(u) && Et.get(u).contains(v)) || (Et.containsKey(v) && Et.get(v).contains(u)))
			Et.get(w).add(w);

		cw = calc_cost(w, Et, Vt); // calculating cost of the merged node

		//System.out.println("u "+u+" "+cu+" v "+v+" "+cv+" w "+cw+" "+Et.get(w));

		if(Et.get(w).contains(w))
			Et.get(w).remove(w);
		if(Et.containsKey(v))
			for(Long nv: Et.get(v))
				Et.get(nv).add(v);
		if(Et.containsKey(u))
			for(Long nu: Et.get(u))
				Et.get(nu).add(u);
		if(Et.containsKey(w))
			for(Long nw: Et.get(w))
				Et.get(nw).remove(w);
		Et.remove(w);

		Vt.remove(w);
		Vt.add(u);
		Vt.add(v);
		superNodeElements.remove(w);
		//w.delete();
		return (cu + cv - cw)/(cu + cv);
	}

	/**
	 * 初始化
	 * @throws Exception
	 */
	void initialize() throws Exception
	{
		//int i=0;
		for(Node n: G.getAllNodes()){
			//if(i>50)
			// break;
			//i++;
			Set<Long> visited = new HashSet<Long>();
			visited.add(n.getId());
			for(Relationship r: n.getRelationships()){
				for(Relationship twoHopR: r.getOtherNode(n).getRelationships()){
					Node twoHopN = twoHopR.getOtherNode(r.getOtherNode(n));
					if(!visited.contains(twoHopN.getId()) && !scost.isAvailable(n.getId(), twoHopN.getId())){
						Double redn_cost = calc_scost(n.getId(), twoHopN.getId(), edgesDeepCopy(Es), new HashSet<Long>(Vs));
						if(redn_cost > 0.0){
							maxHeap.add(new NodePair(n.getId(), twoHopN.getId(), redn_cost));
							scost.put(n.getId(), twoHopN.getId(), redn_cost);
						}
						visited.add(twoHopN.getId());
					}
				}
			}
		}
	}

// public String graphNodes2String(TreeMap<Node, TreeSet<Relationship>> G) {
//    StringBuffer sb = new StringBuffer() ;
//
//
//
//    return sb.toString() ;
// }

	HashMap<Long, HashSet<Long>> edgesDeepCopy(Map<Long, HashSet<Long>> hm){
		HashMap<Long, HashSet<Long>> temp = new HashMap<Long, HashSet<Long>>();
		for(long key: hm.keySet()){
			temp.put(key, new HashSet<Long>(hm.get(key)));
		}
		return temp;
	}

	HashSet<Long> get_neighbors(long w, HashSet<Long> Vt) throws Exception
	{
		HashSet<Long> Nw = new HashSet<Long>();
		for(long v : Vt){
			//if(v == w)
			// continue;
			for(long sub_w: superNodeElements.get(w)){
				for(long sub_v: superNodeElements.get(v)){
					if(Eg.get(sub_w).contains(sub_v)){
						Nw.add(v);
						break;
					}
				}
			}
		}
		return Nw;
	}

	/**
	 * 合并图顶点
	 * @throws Exception
	 */
	void merge() throws Exception
	{
		int old_size = Vs.size();
		while(!maxHeap.isEmpty()){
			//System.out.println("merge "+maxHeap.size());
			//System.out.println("before merge: "+maxHeap);
			NodePair uv = maxHeap.poll();
			System.out.println(uv.toString());
			//Node u = G.getNodeById(uv.u);
			//Node v = G.getNodeById(uv.v);
			Node w = G.createNode();

			superNodeElements.put(w.getId(), new HashSet<Long>());
			superNodeElements.get(w.getId()).addAll(superNodeElements.get(uv.u));
			superNodeElements.get(w.getId()).addAll(superNodeElements.get(uv.v));

			System.out.println("Super nodes: "+Vs);
			System.out.println("size of everytime:"+Vs.size());
			//update super nodes
			Vs.add(w.getId());
			Vs.remove(uv.u);
			Vs.remove(uv.v);
			//update super edges
			Es.put(w.getId(), new HashSet<Long>());
			HashSet<Long> neigh = get_neighbors(w.getId(), new HashSet<Long>(Vs));//new HashSet<Long>();
			if(Es.containsKey(uv.u) && !Es.get(uv.u).isEmpty())
				neigh.addAll(Es.get(uv.u));
			if(Es.containsKey(uv.v) && !Es.get(uv.v).isEmpty())
				neigh.addAll(Es.get(uv.v));
			neigh.remove(uv.u); neigh.remove(uv.v);
			for( Long n : neigh)
			{
				double actual_edges = 0.0;
				double pi = superNodeElements.get(w.getId()).size() * superNodeElements.get(n).size();
				for(Long su: superNodeElements.get(w.getId())){
					for(Long sv: superNodeElements.get(n)){
						//System.out.println("su "+su+" sx"+sx);
						if(Eg.get(su).contains(sv))
							actual_edges++;
					}
				}

				if(actual_edges > ((pi + 1)/2)){
					Es.get(w.getId()).add(n);
				}
			}

			//System.out.println(w.getId()+" edges "+Es.get(w.getId()));
			for(Long nw: Es.get(w.getId())){
				Es.get(nw).add(w.getId());
			}

			if(Es.containsKey(uv.u)){
				for(Long nu: Es.get(uv.u)){
					if(nu != uv.u)    // avoid removing self edge
						Es.get(nu).remove(uv.u);
				}
			}
			if(Es.containsKey(uv.v)){
				for(Long nv: Es.get(uv.v)){
					if(nv != uv.v)    // avoid removing self edge
						Es.get(nv).remove(uv.v);
				}
			}

			List<NodePair> addList = new ArrayList<NodePair>();
			List<NodePair> removeList = new ArrayList<NodePair>();
			//System.out.println(Es);
			for(long x : Vs){
				if(x == w.getId())
					continue;
				if(Es.containsKey(uv.u))
				{
					for(long oneHop : get_neighbors(uv.u, new HashSet<Long>(Vs))){ //Es.get(uv.u)){
						if(get_neighbors(oneHop, new HashSet<Long>(Vs)).contains(x)){ //if(Es.get(oneHop).contains(x)){
							for(NodePair e: maxHeap){
								if((e.u == x || e.v == x) && (e.u == uv.u || e.v == uv.u)){
									removeList.add(e);
									if(!scost.isAvailable(w.getId(), x)){
										double redn_cost = calc_scost(w.getId(), x, edgesDeepCopy(Es), new HashSet<Long>(Vs));
										if(redn_cost >= 0.0){
											addList.add(new NodePair(w.getId(), x, redn_cost));
											scost.put(w.getId(), x, redn_cost);
										}
									}
								}
								if((e.u == oneHop || e.v == oneHop) && (e.u == uv.u || e.v == uv.u)){
									removeList.add(e);
									if(!scost.isAvailable(w.getId(), oneHop)){
										double redn_cost = calc_scost(w.getId(), oneHop, edgesDeepCopy(Es), new HashSet<Long>(Vs));
										if(redn_cost >= 0.0){
											addList.add(new NodePair(w.getId(), oneHop, redn_cost));
											scost.put(w.getId(), oneHop, redn_cost);
										}
									}
								}
							}
						}
					}
				}
				if(Es.containsKey(uv.v))
				{
					for(long oneHop : get_neighbors(uv.v, new HashSet<Long>(Vs))){//Es.get(uv.v)){
						if(get_neighbors(oneHop, new HashSet<Long>(Vs)).contains(x)){ //if(Es.get(oneHop).contains(x)){
							for(NodePair e: maxHeap){
								if ((e.u == x || e.v == x) && (e.u == uv.v || e.v == uv.v)){
									removeList.add(e);
									if(!scost.isAvailable(w.getId(), x))
									{
										double redn_cost = calc_scost(w.getId(), x, edgesDeepCopy(Es), new HashSet<Long>(Vs));
										if(redn_cost >= 0.0){
											addList.add(new NodePair(w.getId(), x, redn_cost));
											scost.put(w.getId(), x, redn_cost);
										}
									}
								}
								if((e.u == oneHop || e.v == oneHop) && (e.u == uv.v || e.v == uv.v)){
									removeList.add(e);
									if(!scost.isAvailable(w.getId(), oneHop)){
										double redn_cost = calc_scost(w.getId(), oneHop, edgesDeepCopy(Es), new HashSet<Long>(Vs));
										if(redn_cost >= 0.0){
											addList.add(new NodePair(w.getId(), oneHop, redn_cost));
											scost.put(w.getId(), oneHop, redn_cost);
										}
									}
								}
							}
						}
					}
				}
			}
         /*
         for(long oneHop : get_neighbors(uv.u, new HashSet<Long>(Vs))){ //Es.get(uv.u)){
            //System.out.println("oneHop "+oneHop);
            //System.out.println(Es.get(oneHop));
            //System.out.println(Es);
            for(long twoHop : get_neighbors(oneHop, new HashSet<Long>(Vs))){ //Es.get(oneHop)){
               //System.out.println("twoHop"+twoHop);
               if(!Vs.contains(twoHop) || twoHop == w.getId())
                  continue;
               for(NodePair e: maxHeap){
                  if((e.u == twoHop || e.v == twoHop) && (e.u == uv.u || e.v == uv.u)){
                     removeList.add(e);
                     double redn_cost = calc_scost(w.getId(), twoHop, edgesDeepCopy(Es), new HashSet<Long>(Vs));

                     if(redn_cost > 0.0){
                        addList.add(new NodePair(w.getId(), twoHop, redn_cost));
                        scost.put(w.getId(), twoHop, redn_cost);
                     }
                  }
                  //System.out.println("NodePair"+e.toString());
                  //System.out.println("Es "+Es);
               }
               //System.out.println(twoHop);
               //System.out.println(Es.get(oneHop));
               //System.out.println(Es);
            }
         }
         for(Long oneHop : get_neighbors(uv.v, new HashSet<Long>(Vs))){ //Es.get(uv.v)){
            for(Long twoHop : get_neighbors(oneHop, new HashSet<Long>(Vs))){ // Es.get(oneHop)){
               if(!Vs.contains(twoHop) || twoHop == w.getId())
                  continue;
               for(NodePair e: maxHeap){
                  if ((e.u == twoHop || e.v == twoHop) && (e.u == uv.v || e.v == uv.v)){
                     removeList.add(e);
                     double redn_cost = calc_scost(w.getId(), twoHop, edgesDeepCopy(Es), new HashSet<Long>(Vs));
                     if(redn_cost > 0.0){
                        addList.add(new NodePair(w.getId(), twoHop, redn_cost));
                        scost.put(w.getId(), twoHop, redn_cost);
                     }
                  }
               }
            }
         }
         */
			maxHeap.removeAll(removeList);
			maxHeap.addAll(addList);
			addList = new ArrayList<NodePair>();
			removeList = new ArrayList<NodePair>();

			for(NodePair e: maxHeap){
				HashSet<Long> Nw = get_neighbors(w.getId(), new HashSet<Long>(Vs));
				//if(Es.get(w.getId()).contains(e.u) || Es.get(w.getId()).contains(e.v)){
				if(Nw.contains(e.u) || Nw.contains(e.v)){
					removeList.add(e);
					double redn_cost = calc_scost(e.u, e.v, edgesDeepCopy(Es), new HashSet<Long>(Vs));
					if( redn_cost >= 0.0 ){
						addList.add(new NodePair(e.u, e.v, redn_cost));
						scost.set(e.u, e.v, redn_cost);
					}
				}
			}
			//self edge
			if( (Es.containsKey(uv.u) && Es.get(uv.u).contains(uv.v)) || (Es.containsKey(uv.v) && Es.get(uv.v).contains(uv.u)))
				Es.get(w.getId()).add(w.getId());

			//if(!removeList.isEmpty())
			maxHeap.removeAll(removeList);
			//if(!addList.isEmpty())
			maxHeap.addAll(addList);

			Es.remove(uv.u);
			Es.remove(uv.v);
			OutputStream output = new FileOutputStream("out/readme.txt");

			//System.out.println("after merge: "+maxHeap);
			//System.out.println("Super nodes: "+Vs);//打印每一次合并后的超级节点
			System.out.println("Super Edges: "+Es);//打印每一次合并后的超级边
         /*for (Map.Entry<Long, HashSet<Long>> entry : Es.entrySet()) {//将map格式的节点信息分开,遍历map内容
            Long key = entry.getKey();
            HashSet<Long> value = entry.getValue();
            System.out.println(key + " = " + value);
         }*/


			System.out.println(superNodeElements);

			if(old_size<Vs.size())
				break;
			else
				old_size = Vs.size();
			//System.out.println("");
		}
		System.out.println("Super nodes: "+Vs);
		System.out.println("Super Edges: "+Es);
		System.out.println(superNodeElements);
	}

	void output(){
		List<Long> list_Vs = new ArrayList<Long>(Vs);
		for(int i=0; i<list_Vs.size()-1; i++){
			for(int j=i+1; j<list_Vs.size(); j++){
				long u = list_Vs.get(i);
				long v = list_Vs.get(j);
				double actual_edges = 0.0;
				double pi = superNodeElements.get(u).size() * superNodeElements.get(v).size();
				for(Long su: superNodeElements.get(u)){
					for(Long sv: superNodeElements.get(v)){
						//System.out.println("su "+su+" sx"+sx);
						if(Eg.get(su).contains(sv))
							actual_edges++;
					}
				}

				if(actual_edges > ((pi + 1)/2)){
					if(ES.containsKey(u))
						ES.get(u).add(v);
					else{
						ES.put(u, new HashSet<Long>());
						ES.get(u).add(v);
					}
					if(ES.containsKey(v))
						ES.get(v).add(u);
					else{
						ES.put(v, new HashSet<Long>());
						ES.get(v).add(u);
					}

					for(Long su: superNodeElements.get(u)){
						for(Long sv: superNodeElements.get(v)){
							//System.out.println("su "+su+" sx"+sx);
							if(!Eg.get(su).contains(sv)){
								if(su < sv){
									if(nC.containsKey(su))
										nC.get(su).add(sv);
									else{
										nC.put(su, new HashSet<Long>());
										nC.get(su).add(sv);
									}
								}
								else{
									if(nC.containsKey(sv))
										nC.get(sv).add(su);
									else{
										nC.put(sv, new HashSet<Long>());
										nC.get(sv).add(su);
									}
								}
							}
						}
					}
				}
				else{
					for(Long su: superNodeElements.get(u)){
						for(Long sv: superNodeElements.get(v)){
							//System.out.println("su "+su+" sx"+sx);
							if(Eg.get(su).contains(sv)){
								if(su < sv){
									if(pC.containsKey(su))
										pC.get(su).add(sv);
									else{
										pC.put(su, new HashSet<Long>());
										pC.get(su).add(sv);
									}
								}
								else{
									if(pC.containsKey(sv))
										pC.get(sv).add(su);
									else{
										pC.put(sv, new HashSet<Long>());
										pC.get(sv).add(su);
									}
								}
							}
						}
					}
				}
			}

		}
		System.out.println("positive constraints: "+pC);
		System.out.println("negative constraints: "+nC);
		System.out.println("ES: "+ES);
	}

	public static void main(String args[]) throws Exception {
		//File storeDir = new File("C:\\Users\\Giridhar\\Documents\\Neo4j\\movies.db");
		//File storeDir = new File("C:\\Users\\Giridhar\\Documents\\Neo4j\\tempGS1.graphdb");
		File storeDir = new File("D:\\downloads\\neo4j-community-3.5.30\\data\\databases\\graph.db");
//    File storeDir = new File("D:/OpenSourceSystem-Code/RDF-Sum2/facebook_combined.csv");

//    String path = "E:\\neo4j-community-3.5.5\\data\\databases\\graph.db" ;
		//System.out.println(""+sourceDir.getAbsolutePath()+"\n"+storeDir.getAbsolutePath());
		//copyFiles(sourceDir, storeDir);

		String path = "D:\\downloads\\neo4j-community-3.5.30\\data\\databases\\graph.db";
		//System.out.println(""+sourceDir.getAbsolutePath()+"\n"+storeDir.getAbsolutePath());
		//copyFiles(sourceDir, storeDir);

		GraphDatabaseService tempGs = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(storeDir)
				//.setConfig(GraphDatabaseSettings.pagecache_memory, "1g")
				//原文未注释,没有pagecache_memory这个参数
				.setConfig(GraphDatabaseSettings.allow_store_upgrade, "true")
				.newGraphDatabase();


		Transaction tx = tempGs.beginTx();


		/**
		 * 导入图数据
		 */
//    try
//    {
//       Node node_a= tempGs.createNode();
//       node_a.setProperty("sx","zhuang");
//       node_a.addLabel(Tutorials.honglou);
//       Node node_b= tempGs.createNode();
//       node_b.setProperty("name","b");
//       node_b.addLabel(Tutorials.honglou);
//       Node node_c= tempGs.createNode();
//       node_c.setProperty("name","c");
//       node_a.addLabel(Tutorials.honglou);
//       Node node_d= tempGs.createNode();
//       node_d.setProperty("name","d");
//       Node node_e= tempGs.createNode();
//       node_e.setProperty("name","e");
//       Node node_f= tempGs.createNode();
//       node_f.setProperty("name","f");
//       Node node_g= tempGs.createNode();
//       node_g.setProperty("name","g");
//       Node node_h= tempGs.createNode();
//       node_h.setProperty("name","h");
//       node_d.addLabel(Tutorials.honglou);
//       node_e.addLabel(Tutorials.honglou);
//       node_f.addLabel(Tutorials.honglou);
//       node_g.addLabel(Tutorials.honglou);
//       node_h.addLabel(Tutorials.honglou);
//
//
//
//            System.out.println( "node_a:" + node_a.toString());
//            System.out.println( "node_b:" + node_b.toString());
//            System.out.println( "node_c:" + node_c.toString());
//            System.out.println( "node_d:" + node_d.toString());
//            System.out.println( "node_e:" + node_e.toString());
//            System.out.println( "node_f:" + node_f.toString());
//            System.out.println( "node_g:" + node_g.toString());
//            System.out.println( "node_h:" + node_h.toString());
//
//       /**
//        * a的边
//        */
//       Relationship rel_a1 = node_a.createRelationshipTo(tempGs.getNodeById(node_b.getId()) ,new TestRelationshipType("R"));
//       Relationship rel_a2 = node_a.createRelationshipTo(tempGs.getNodeById(node_c.getId()) ,new TestRelationshipType("R"));
//       Relationship rel_a3 = node_a.createRelationshipTo(tempGs.getNodeById(node_g.getId()) ,new TestRelationshipType("R"));
//       Relationship rel_a4 = node_a.createRelationshipTo(tempGs.getNodeById(node_h.getId()) ,new TestRelationshipType("R"));
//
//       /**
//        * b的边
//        */
//       Relationship rel4 = node_b.createRelationshipTo(tempGs.getNodeById(node_a.getId()) ,new TestRelationshipType("R"));
//       Relationship rel5 = node_b.createRelationshipTo(tempGs.getNodeById(node_c.getId()) ,new TestRelationshipType("R"));
//
//       /**
//        * c的边
//        */
//       Relationship rel6 = node_c.createRelationshipTo(tempGs.getNodeById(node_a.getId()) ,new TestRelationshipType("R"));
//       Relationship rel7 = node_c.createRelationshipTo(tempGs.getNodeById(node_b.getId()) ,new TestRelationshipType("R"));
//
//       /**
//        * d的边
//        */
//       Relationship rel8 = node_d.createRelationshipTo(tempGs.getNodeById(node_h.getId()) ,new TestRelationshipType("R"));
//
//       /**
//        * e的边
//        */
//       Relationship rel9  = node_e.createRelationshipTo(tempGs.getNodeById(node_a.getId()) ,new TestRelationshipType("R"));
//       Relationship rel10 = node_e.createRelationshipTo(tempGs.getNodeById(node_g.getId()) ,new TestRelationshipType("R"));
//       Relationship rel11 = node_e.createRelationshipTo(tempGs.getNodeById(node_h.getId()) ,new TestRelationshipType("R"));
//
//       /**
//        * f的边
//        */
//       Relationship rel12 = node_f.createRelationshipTo(tempGs.getNodeById(node_g.getId()) ,new TestRelationshipType("R"));
//       Relationship rel13 = node_f.createRelationshipTo(tempGs.getNodeById(node_h.getId()) ,new TestRelationshipType("R"));
//
//       /**
//        * g的边
//        */
//       Relationship rel14 = node_g.createRelationshipTo(tempGs.getNodeById(node_a.getId()) ,new TestRelationshipType("R"));
//       Relationship rel15 = node_g.createRelationshipTo(tempGs.getNodeById(node_e.getId()) ,new TestRelationshipType("R"));
//       Relationship rel19 = node_g.createRelationshipTo(tempGs.getNodeById(node_f.getId()) ,new TestRelationshipType("R"));
//
//       /**
//        * h的边
//        */
//       Relationship rel16 = node_h.createRelationshipTo(tempGs.getNodeById(node_a.getId()) ,new TestRelationshipType("R"));
//       Relationship rel17 = node_h.createRelationshipTo(tempGs.getNodeById(node_d.getId()) ,new TestRelationshipType("R"));
//       Relationship rel18 = node_h.createRelationshipTo(tempGs.getNodeById(node_e.getId()) ,new TestRelationshipType("R"));
//       Relationship rel20 = node_h.createRelationshipTo(tempGs.getNodeById(node_f.getId()) ,new TestRelationshipType("R"));
//    } catch (Exception e )
//    {
//       e.printStackTrace();
//    }

		System.out.println(tempGs.toString()); //neo4j接口

		GreedySummary gs = new GreedySummary(tempGs);
		gs.initialize();
		System.out.println("No. of nodes before merge: "+gs.Vg.size());
		System.out.println("Nodes: "+gs.Vg);
		//gs.scost.print();
		//System.out.println(gs.maxHeap.peek().toString());
		//System.out.println(gs.Eg.toString());
		long startTime = System.currentTimeMillis();
		System.out.println("start time: "+startTime);
		gs.merge();
		gs.output(); //
		long endTime = System.currentTimeMillis();
		System.out.println("end time: "+endTime);
		System.out.println("Running Time (in ms): "+(endTime - startTime));
		System.out.println("No. of nodes after merge: "+gs.Vs.size());
		System.out.println("done");
		//System.out.println(gs.Eg);
		tx.success();

		tempGs.shutdown();
	}
	private static void registerShutdownHook(final GraphDatabaseService graphDB) {
		// Registers a shutdown hook for the Neo4j instance so that it shuts down nicely
		// when the VM exits (even if you "Ctrl-C" the running example before it's completed)
		/*为了确保neo4j数据库的正确关闭，我们可以添加一个关闭钩子方法 registerShutdownHook。
		 *这个方法的意思就是在jvm中增加一个关闭的钩子，
		 *当jvm关闭的时候，会执行系统中已经设置的所有通过方法addShutdownHook添加的钩子，
		 *当系统执行完这些钩子后，jvm才会关闭。
		 *所以这些钩子可以在jvm关闭的时候进行内存清理、对象销毁等操作。*/
		Runtime.getRuntime().addShutdownHook(
				new Thread() {
					public void run() {
						//Shutdown the Database
						System.out.println("Server is shutting down");
						graphDB.shutdown();
					}
				}
		);
	}
}
