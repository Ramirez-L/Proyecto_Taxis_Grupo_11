import './App.css';
import Boton from './Componentes/Boton';
import Tabla from './Componentes/Tabla/Tabla';
import Opciones from './Componentes/Opciones/opciones'

function App() {
  return (
    <div className="App">
      <header className="App-header">
      <h3 style={{"marginTop": "20px"}}>Tablas Rapidas</h3>
      <p style={{"marginTop": "10px"}}>Selecciona una tabla</p>
      <Opciones></Opciones>
      <Boton></Boton>
      <Tabla></Tabla>
      </header>
    </div>
  );
}

export default App;
