(
'Red=rgb(185, 22, 22)',
'Green=rgb(22, 185, 36)',
'Orange=rgb(228, 134, 47)',
'Pink=rgb(212, 40, 218)',
'Purple=rgb(81, 44, 218)',
'Gray=rgb(151, 151, 151)'
) | %{
    $name = $_.Split('=')[0]
    $rgb = $_.Split('=')[1]
    dir *-Blue.svg | %{
        (gc $_ -raw).Replace('#0070FF', $rgb).Replace('#0070ff', $rgb) | out-file $_.Name.Replace('Blue', $name)
    }
}