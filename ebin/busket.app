{application, busket,
 [
  {description, "Service monitoring"},
  {vsn, "1"},
  {modules, [
             busket_app,
             busket_sup,
             busket,
             interface_udp,
             store,
             store_debug,
             store_mongo
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib,
                  sasl,
                  emongo
                 ]},
  {mod, { busket_app, []}},
  {env, []}
 ]}.
