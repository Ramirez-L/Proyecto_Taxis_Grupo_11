// Memoria local o datos base para ser mostrados en pantalla
const initialState = {
    // Query
    query: '',

    // datos
    datos: [{'Columna': "Fila"}],

    // Consulta
    consulta: '',

    // Consultas rapidas
    quick: [
        {"trip": "select * from trip limit 5"},
        {"borough": "select * from borough"},
        {"weather": "select * from weather"},
        {"location": "select * from location"},
        {"rate": "select * from rate"},
        {"pay_type": "select * from payment"},
        {"vendor": "select * from vendor"}
    ]

};

let Aux = undefined;

function rootReducer(state = Aux === undefined ? initialState : Aux, action) {
    switch (action.type) {
        case "GET_QUERY":
            return {
                ...state,
                datos: action.payload.data,
                query: action.payload.query,
                consulta: action.payload.consulta
            }

        case "CLEAR_STORE":
            return {
                ...state,
                query: action.payload.query,
                datos: action.payload.datos,
                consulta: action.payload.consulta
            }
   
        default:
            return state;
    }
}

export default rootReducer;