{application, busket,
 [
  {description, "Service monitoring"},
  {vsn, "1"},
  {modules, [
             busket_app,
             busket_sup,
             busket,
             busket_store,
             busket_store_debug,
             busket_store_mongo,
             busket_interface_udp
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
