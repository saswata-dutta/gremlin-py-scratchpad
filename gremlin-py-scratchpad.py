from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T
from gremlin_python.process.traversal import Cardinality as C
from gremlin_python.process.traversal import Column as Col
from gremlin_python.process.traversal import Scope as S
from gremlin_python.process.traversal import lt, gt, Order, inside
from gremlin_python.process.graph_traversal import __, unfold, flatMap, out, in_


EDGES = ["p_u", "i_u"]


def populate(g):
    (g.
    addV("o").property(T.id, 0).property(C.single, "ts", 9).property(C.single, "cid", "c1").as_("o0").

    addV("o").property(T.id, 1).property(C.single, "ts", 1).property(C.single, "cid", "c1").as_("o1").
    addV("o").property(T.id, 2).property(C.single, "ts", 2).property(C.single, "cid", "c1").as_("o2").

    addV("o").property(T.id, 3).property(C.single, "ts", 1).property(C.single, "cid", "c2").as_("o3").
    addV("o").property(T.id, 4).property("ts", 2).property(C.single, "cid", "c2").as_("o4").

    addV("o").property(T.id, 5).property(C.single, "ts", 1).property(C.single, "cid", "c3").as_("o5").
    addV("o").property(T.id, 6).property(C.single, "ts", 3).property(C.single, "cid", "c3").as_("o6").

    addV("p").property(T.id, 100).as_("p0").
    addV("i").property(T.id, 200).as_("i0").

    addE("p_u").from_("p0").to("o0").
    addE("p_u").from_("p0").to("o1").
    addE("p_u").from_("p0").to("o2").
    addE("p_u").from_("p0").to("o3").
    addE("p_u").from_("p0").to("o4").
    addE("p_u").from_("p0").to("o5").
    addE("p_u").from_("p0").to("o6").

    addE("i_u").from_("i0").to("o0").
    addE("i_u").from_("i0").to("o1").
    addE("i_u").from_("i0").to("o2").
    addE("i_u").from_("i0").to("o3").
    addE("i_u").from_("i0").to("o4").
    addE("i_u").from_("i0").to("o5").
    addE("i_u").from_("i0").to("o6").

    iterate())


def drop(g):
    g.V().drop().iterate()


def query(g, vid):
    return (g.with_('evaluationTimeout', 30000).
    V(vid).as_("hop_0").
    in_(*EDGES).as_("hop_1").
    local(flatMap(
        out(*EDGES).where(lt("hop_0")).by("ts").
        group().by("cid").
        sample(S.local, 2).select(Col.values).unfold().
        order(S.local).by("ts", Order.desc).limit(S.local, 1)).as_("hop_2")).
    optional(
        __.in_(*EDGES).as_("hop_3").
        local(flatMap(
            out(*EDGES).where(lt("hop_2")).by("ts").
            group().by("cid").
            sample(S.local, 2).select(Col.values).unfold().
            order(S.local).by("ts", Order.desc).limit(S.local, 1)).as_("hop_4"))).
    path().by(T.id).from_("hop_1").
    toList())


def simple(g, vid):
    return g.with_('evaluationTimeout', 30000).V(vid).as_("hop_0").in_().out().path().by(T.id).from_("hop_0").toList()


def main():
    conn = None
    try:
        conn = DriverRemoteConnection('ws://localhost:8182/gremlin','g')
        g = traversal().withRemote(conn)
        drop(g)
        populate(g)
        print(simple(g, 0))
        print()
        print(query(g, 0))
    finally:
        if conn:
            conn.close()



if __name__ == '__main__':
    main()
