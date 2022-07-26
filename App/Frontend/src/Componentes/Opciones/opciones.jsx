import * as React from 'react';
import { useSelector, useDispatch } from "react-redux";
import Box from '@mui/material/Box';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select from '@mui/material/Select';
import { Quick_query } from '../../redux/actions';

export default function Opciones() {
  // Declaracion para luego ejecutar las actions.js
    const dispatch = useDispatch()
    
    // Tomar los datos base de la memoria local Redux
    const [tabla, setTabla] = React.useState('');
    var quick = useSelector(state => state.quick);
    
  const handleChange = (event) => {
      event.preventDefault()
      setTabla(event.target.value);

      // Tomar solo el valor Seleccionado y la consulta pre-guardada de la memoria local Redux
      var consulta = Object.values(quick[event.target.value])[0]

      // Enviar la consulta pre-guardada
      dispatch(Quick_query(consulta))
  };

  return (
    <Box sx={{ minWidth: 120 }} style={{"marginTop": "10px"}}>
      <FormControl fullWidth size="medium">
        <InputLabel id="demo-simple-select-label">Tabla</InputLabel>
        <Select
          labelId="demo-simple-select-label"
          id="demo-simple-select"
          value={tabla}
          label="Tabla"
          onChange={handleChange}
        >
          <MenuItem value={"0"}>trip</MenuItem>
          <MenuItem value={"1"}>borough</MenuItem>
          <MenuItem value={"2"}>weather</MenuItem>
          <MenuItem value={"3"}>location</MenuItem>
          <MenuItem value={"4"}>rate</MenuItem>
          <MenuItem value={"5"}>pay_type</MenuItem>
          <MenuItem value={"6"}>vendor</MenuItem>
        </Select>
      </FormControl>
    </Box>
  );
}
